import { closeSync, existsSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
//#region runtime.ts
var tables = {
	api_cache: "cache_key TEXT PRIMARY KEY,payload TEXT NOT NULL,expires_at INTEGER NOT NULL",
	weather_snapshots: "region_id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL",
	collection_state: "id TEXT PRIMARY KEY,next_due INTEGER NOT NULL,lease_until INTEGER NOT NULL,last_started INTEGER NOT NULL,last_finished INTEGER,last_error TEXT",
	warning_snapshot: "id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL",
	population_snapshot: "id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL",
	population_jobs: "id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL",
	commerce_snapshot: "id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL",
	commerce_pages: "id TEXT PRIMARY KEY,run_id TEXT NOT NULL,page INTEGER NOT NULL,payload TEXT NOT NULL",
	commerce_state: "id TEXT PRIMARY KEY,payload TEXT NOT NULL,saved_at INTEGER NOT NULL"
};
var JsonDatabase = class {
	sql = new DatabaseSync(":memory:");
	constructor() {
		for (const [t, s] of Object.entries(tables)) this.sql.exec(`CREATE TABLE ${t} (${s})`);
	}
	prepare(query) {
		const stmt = this.sql.prepare(query);
		let args = [];
		const api = {
			bind(...v) {
				args = v.map((x) => x === void 0 ? null : x);
				return api;
			},
			async first(column) {
				const row = stmt.get(...args);
				return row ? column ? row[column] : row : null;
			},
			async all() {
				return {
					results: stmt.all(...args),
					success: true
				};
			},
			async run() {
				const r = stmt.run(...args);
				return {
					success: true,
					meta: {
						changes: Number(r.changes),
						last_row_id: Number(r.lastInsertRowid)
					}
				};
			}
		};
		return api;
	}
	async batch(statements) {
		this.sql.exec("BEGIN");
		try {
			const results = [];
			for (const s of statements) results.push(await s.run());
			this.sql.exec("COMMIT");
			return results;
		} catch (e) {
			this.sql.exec("ROLLBACK");
			throw e;
		}
	}
	restore(data) {
		if (data?.version !== 1 || !data.tables || typeof data.tables !== "object") throw Error("수집 상태 JSON 형식 확인 필요");
		for (const [t, rows] of Object.entries(data.tables)) {
			if (!(t in tables) || !Array.isArray(rows)) continue;
			for (const row of rows) {
				const keys = Object.keys(row);
				const allowed = new Set(this.sql.prepare(`PRAGMA table_info(${t})`).all().map((x) => x.name));
				if (!keys.length || keys.some((k) => !allowed.has(k))) throw Error("수집 상태 항목 확인 필요");
				this.sql.prepare(`INSERT INTO ${t} (${keys.join(",")}) VALUES (${keys.map(() => "?").join(",")})`).run(...keys.map((k) => row[k]));
			}
		}
	}
	dump() {
		return {
			version: 1,
			tables: Object.fromEntries(Object.keys(tables).map((t) => [t, this.sql.prepare(`SELECT * FROM ${t}`).all()]))
		};
	}
};
var database = new JsonDatabase();
var env = {
	...process.env,
	DB: database
};
var secretNames = [
	"KMA_API_KEY",
	"AIRKOREA_API_KEY",
	"POPULATION_API_KEY",
	"BIRTH_API_KEY",
	"COMMERCE_API_KEY",
	"TRAFFIC_API_KEY",
	"HOUSEHOLD_SIZE_API_KEY"
];
function redact(value) {
	for (const k of secretNames) {
		const v = process.env[k];
		if (!v || v.length < 8) continue;
		let decoded = v;
		try {
			decoded = decodeURIComponent(v);
		} catch {}
		for (const s of new Set([
			v,
			decoded,
			encodeURIComponent(decoded)
		])) value = value.split(s).join("[비공개]");
	}
	return value.replace(/([?&](?:servicekey|apikey)=)[^&\s"<>]+/gi, "$1[비공개]");
}
function readJSON(path, fallback = null) {
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch (e) {
		if (e.code === "ENOENT") return fallback;
		throw Error("저장 JSON 손상 확인 필요 · 덮어쓰기를 중지합니다.");
	}
}
function writeJSON(path, value) {
	mkdirSync(dirname(path), { recursive: true });
	const temp = path + ".tmp";
	writeFileSync(temp, redact(JSON.stringify(value)));
	renameSync(temp, path);
}
//#endregion
//#region lib/refresh-policy.ts
var HOUR$1 = 36e5;
var DAY$2 = 24 * HOUR$1;
var WARNING_INTERVAL = 30 * 6e4;
function retryDelay(attempt) {
	return [
		5 * 6e4,
		15 * 6e4,
		HOUR$1
	][Math.min(Math.max(0, attempt), 2)];
}
function monthlyTtl(month, latest) {
	return month >= latest ? DAY$2 / 1e3 : 30 * DAY$2 / 1e3;
}
function monthlyBatch(month) {
	return `population-month-v2:${month}`;
}
//#endregion
//#region lib/warnings.ts
var GYEONGGI_CITIES = [
	"수원시",
	"성남시",
	"의정부시",
	"안양시",
	"부천시",
	"광명시",
	"평택시",
	"동두천시",
	"안산시",
	"고양시",
	"과천시",
	"구리시",
	"남양주시",
	"오산시",
	"시흥시",
	"군포시",
	"의왕시",
	"하남시",
	"용인시",
	"파주시",
	"이천시",
	"안성시",
	"김포시",
	"화성시",
	"광주시",
	"양주시",
	"포천시",
	"여주시",
	"연천군",
	"가평군",
	"양평군"
];
var kinds = "강풍|호우|한파|건조|폭풍해일|풍랑|태풍|대설|황사|폭염|열대야|안개|지진해일";
function warningDate(raw) {
	const s = String(raw ?? "");
	return /^\d{12}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}T${s.slice(8, 10)}:${s.slice(10, 12)}:00+09:00` : null;
}
function emptyWarnings() {
	return {
		verified: false,
		collectedAt: null,
		issuedAt: null,
		effectiveAt: null,
		cities: GYEONGGI_CITIES.map((name) => ({
			name,
			active: [],
			preliminary: []
		})),
		rawCurrent: "",
		rawPreliminary: "",
		notes: "",
		parseNotes: [],
		error: "특보 자료 확인 중"
	};
}
function splitAreas(text) {
	let depth = 0, start = 0;
	const result = [];
	for (let i = 0; i < text.length; i++) {
		if (text[i] === "(") depth++;
		if (text[i] === ")") depth--;
		if ((text[i] === "," || text[i] === "，") && depth === 0) {
			result.push(text.slice(start, i).trim());
			start = i + 1;
		}
	}
	result.push(text.slice(start).trim());
	return result.filter(Boolean);
}
function gyeonggiAreas(text) {
	const matches = [];
	let unresolved = false;
	for (const match of text.matchAll(/경기도/g)) {
		let i = match.index + 3;
		while (/\s/.test(text[i] ?? "") && i < text.length) i++;
		if (text[i] === "(") {
			let depth = 1, start = ++i;
			for (; i < text.length && depth; i++) {
				if (text[i] === "(") depth++;
				if (text[i] === ")") depth--;
			}
			if (depth) {
				unresolved = true;
				continue;
			}
			matches.push(text.slice(start, i - 1));
		} else if (!text[i] || /[,，\s.]/.test(text[i])) matches.push("전체");
		else unresolved = true;
	}
	return {
		matches,
		unresolved
	};
}
function parseWarningStatus(row, now) {
	if (typeof row.t6 !== "string" || !row.t6.trim()) throw new Error("특보 발효현황 내용이 없습니다.");
	const data = emptyWarnings();
	data.verified = true;
	data.error = null;
	data.collectedAt = now.toISOString();
	data.issuedAt = warningDate(row.tmFc);
	data.effectiveAt = warningDate(row.tmEf);
	data.rawCurrent = row.t6.replace(/\\n/g, "\n");
	data.rawPreliminary = String(row.t7 ?? "").replace(/\\n/g, "\n");
	data.notes = String(row.other ?? "");
	function apply(text, preliminary) {
		const regex = new RegExp(`(${kinds})\\s*(중대경보|경보|주의보|예비특보)\\s*[:：]?`, "g");
		const headings = [...text.matchAll(regex)];
		if (!preliminary && /경기도/.test(text) && !headings.length) data.parseNotes.push("경기도 특보 형식 확인 필요");
		headings.forEach((m, index) => {
			const kind = m[1], level = preliminary ? "예비특보" : m[2];
			if (!preliminary && level === "예비특보") return;
			const section = text.slice(m.index + m[0].length, headings[index + 1]?.index ?? text.length);
			const { matches, unresolved } = gyeonggiAreas(section);
			if (unresolved) data.parseNotes.push(`${kind} ${level} 구역 확인 필요`);
			const targets = /* @__PURE__ */ new Map();
			if (/경기(?:북부|남부)/.test(section) && !matches.length) data.parseNotes.push(`${kind} ${level} 광역 구역 확인 필요`);
			if (!matches.length) for (const name of GYEONGGI_CITIES) {
				const found = section.match(new RegExp(`(?:^|[\\s,(])(${name}(?:[동서남북][^,()\\s]*)?)(?=$|[,()\\s])`));
				if (found) targets.set(name, [found[1]]);
			}
			for (const list of matches) {
				const excluded = /제외/.test(list);
				const areas = splitAreas(list);
				if (list === "전체") {
					for (const name of GYEONGGI_CITIES) targets.set(name, ["경기도 전체"]);
					continue;
				}
				if (excluded && !/\([^)]*제외\)/.test(list)) {
					const excludedCities = GYEONGGI_CITIES.filter((name) => list.includes(name) || list.includes(name.slice(0, -1)));
					for (const name of GYEONGGI_CITIES) if (!excludedCities.includes(name)) targets.set(name, ["경기도 전체(일부 시군 제외)"]);
					continue;
				}
				for (const area of areas) {
					const city = GYEONGGI_CITIES.find((name) => area.replace(/\s/g, "").startsWith(name) || area.replace(/\s/g, "").startsWith(name.slice(0, -1)));
					if (city) {
						const names = targets.get(city) ?? [];
						names.push(area);
						targets.set(city, names);
					} else data.parseNotes.push(`${kind} ${level}: ${area} 구역 확인 필요`);
				}
			}
			for (const city of data.cities) {
				const areas = targets.get(city.name);
				if (!areas) continue;
				const entries = preliminary ? city.preliminary : city.active;
				const previous = entries.find((x) => x.kind === kind && x.level === level);
				if (previous) previous.areas = [...new Set([...previous.areas, ...areas])];
				else entries.push({
					kind,
					level,
					areas: [...new Set(areas)],
					timing: preliminary ? section.split(/[:：]/)[0].replace(/^[\s\no○]+/, "").trim() : data.effectiveAt ?? ""
				});
			}
		});
	}
	apply(data.rawCurrent, false);
	apply(data.rawPreliminary, true);
	data.parseNotes = [...new Set(data.parseNotes)];
	if (data.parseNotes.length) data.verified = false;
	return data;
}
//#endregion
//#region lib/warning-store.ts
function db$4() {
	if (!env.DB) throw new Error("특보 저장소 연결 확인 중");
	return env.DB;
}
async function readWarnings() {
	const row = await db$4().prepare("SELECT payload FROM warning_snapshot WHERE id='gyeonggi'").first();
	return row ? JSON.parse(row.payload) : null;
}
async function collectWarnings() {
	const started = Date.now();
	if (!(await db$4().prepare("INSERT INTO collection_state (id,next_due,lease_until,last_started,last_finished,last_error) VALUES ('warnings',0,?,?,NULL,NULL) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND collection_state.next_due<=? AND collection_state.last_started<=?").bind(started + 6e4, started, started, started, started - 18e5).run()).meta.changes) return;
	let error = null;
	const previous = await readWarnings();
	let data = emptyWarnings();
	try {
		const bindings = env;
		if (!bindings.KMA_API_KEY) throw new Error("기상청 인증키 설정 필요");
		let key = bindings.KMA_API_KEY;
		try {
			key = decodeURIComponent(key);
		} catch {}
		const url = new URL("https://apis.data.go.kr/1360000/WthrWrnInfoService/getPwnStatus");
		for (const [k, v] of Object.entries({
			serviceKey: key,
			numOfRows: "10",
			pageNo: "1",
			dataType: "JSON"
		})) url.searchParams.set(k, v);
		const response = await fetch(url, { signal: AbortSignal.timeout(8e3) });
		if (!response.ok) throw new Error("기상특보 제공기관 응답 지연");
		const json = await response.json();
		const head = json.response?.header;
		if (!["00", "0"].includes(String(head?.resultCode))) throw new Error("기상특보 현황 자료 확인 중");
		const rows = json.response?.body?.items?.item;
		const items = Array.isArray(rows) ? rows : rows ? [rows] : [];
		items.sort((a, b) => String(b.tmFc).localeCompare(String(a.tmFc)));
		if (!items.length) throw new Error("제공기관의 최근 특보 현황 자료 없음");
		data = parseWarningStatus(items[0], /* @__PURE__ */ new Date());
		if (!data.verified) throw new Error(data.parseNotes.join(" · "));
		if (previous?.issuedAt && data.issuedAt && data.issuedAt < previous.issuedAt) data = {
			...previous,
			collectedAt: (/* @__PURE__ */ new Date()).toISOString(),
			error: null
		};
	} catch {
		error = "기상특보 수집 지연 · 마지막 저장 현황 표시";
		data = previous ? {
			...previous,
			error
		} : {
			...data,
			error
		};
		data.error = error;
	}
	await db$4().batch([db$4().prepare("INSERT INTO warning_snapshot (id,payload,saved_at) VALUES ('gyeonggi',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(JSON.stringify(data), data.collectedAt ? Date.parse(data.collectedAt) : 0), db$4().prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id='warnings' AND last_started=?").bind(started + WARNING_INTERVAL, Date.now(), error, started)]);
}
//#endregion
//#region lib/api-cache.ts
async function readApiCache(key) {
	if (!env.DB) return void 0;
	try {
		const row = await env.DB.prepare("SELECT payload FROM api_cache WHERE cache_key = ? AND expires_at > ?").bind(key, Date.now()).first();
		return row ? JSON.parse(row.payload) : void 0;
	} catch {
		return;
	}
}
async function writeApiCache(key, value, ttl) {
	if (!env.DB) return;
	try {
		await env.DB.prepare("INSERT INTO api_cache (cache_key,payload,expires_at) VALUES (?,?,?) ON CONFLICT(cache_key) DO UPDATE SET payload=excluded.payload,expires_at=excluded.expires_at").bind(key, JSON.stringify(value), Date.now() + ttl * 1e3).run();
	} catch {}
}
//#endregion
//#region lib/regions.ts
var REGIONS = [
	{
		"id": "city",
		"name": "의정부시",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115051000",
		"name": "의정부1동",
		"nx": 61,
		"ny": 131
	},
	{
		"id": "4115052000",
		"name": "의정부2동",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115054500",
		"name": "호원1동",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115055500",
		"name": "호원2동",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115056100",
		"name": "장암동",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115056700",
		"name": "신곡1동",
		"nx": 61,
		"ny": 130
	},
	{
		"id": "4115056800",
		"name": "신곡2동",
		"nx": 61,
		"ny": 131
	},
	{
		"id": "4115057300",
		"name": "송산1동",
		"nx": 62,
		"ny": 130
	},
	{
		"id": "4115063000",
		"name": "고산동",
		"nx": 62,
		"ny": 130
	},
	{
		"id": "4115057600",
		"name": "송산2동",
		"nx": 62,
		"ny": 130
	},
	{
		"id": "4115057800",
		"name": "송산3동",
		"nx": 62,
		"ny": 131
	},
	{
		"id": "4115058000",
		"name": "자금동",
		"nx": 61,
		"ny": 131
	},
	{
		"id": "4115059500",
		"name": "가능동",
		"nx": 61,
		"ny": 131
	},
	{
		"id": "4115061500",
		"name": "흥선동",
		"nx": 61,
		"ny": 131
	},
	{
		"id": "4115062000",
		"name": "녹양동",
		"nx": 61,
		"ny": 131
	}
];
REGIONS[0];
var AIR_STATIONS = {
	"의정부1동": {
		address: "의정부시 가능로152번길 14 의정부1동 작은도서관",
		area: "도심 북부·서부 생활권"
	},
	"의정부동": {
		address: "의정부시 범골로 138 경기도도로사업소",
		area: "도심·남부 생활권"
	},
	"송산3동": {
		address: "의정부시 민락로243번길 94 푸른마당 근린공원 인근",
		area: "송산·고산 생활권"
	}
};
function stationForRegion(region) {
	if ([
		"송산1동",
		"송산2동",
		"송산3동",
		"고산동"
	].includes(region.name)) return "송산3동";
	if ([
		"의정부1동",
		"신곡2동",
		"자금동",
		"가능동",
		"흥선동",
		"녹양동"
	].includes(region.name)) return "의정부1동";
	return "의정부동";
}
//#endregion
//#region lib/weather.ts
var kst = (date) => new Date(date.getTime() + 9 * 36e5);
var pad = (n) => String(n).padStart(2, "0");
function stamp(date) {
	return `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}`;
}
function baseFor(kind, now, back = 0) {
	const d = kst(now);
	if (kind === "getVilageFcst") {
		d.setUTCMinutes(d.getUTCMinutes() - 15);
		const h = [
			23,
			20,
			17,
			14,
			11,
			8,
			5,
			2
		].find((h) => h <= d.getUTCHours());
		if (h === void 0) {
			d.setUTCDate(d.getUTCDate() - 1);
			d.setUTCHours(23, 0, 0, 0);
		} else d.setUTCHours(h, 0, 0, 0);
		d.setUTCHours(d.getUTCHours() - back * 3);
	} else if (kind === "getUltraSrtFcst") {
		d.setUTCMinutes(d.getUTCMinutes() - 45);
		d.setUTCMinutes(30, 0, 0);
		d.setUTCHours(d.getUTCHours() - back);
	} else {
		d.setUTCMinutes(d.getUTCMinutes() - 40);
		d.setUTCMinutes(0, 0, 0);
		d.setUTCHours(d.getUTCHours() - back);
	}
	return {
		date: stamp(d),
		time: pad(d.getUTCHours()) + pad(d.getUTCMinutes())
	};
}
var at = (date, time) => `${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T${time.slice(0, 2)}:${time.slice(2, 4)}:00+09:00`;
function number(v) {
	if (v === void 0 || v === null || v === "" || v === "-" || !Number.isFinite(Number(v))) return null;
	return Number(v);
}
function condition(v) {
	const p = number(v.PTY);
	if (p !== null && p !== 0) return {
		1: "비",
		2: "비·눈",
		3: "눈",
		4: "소나기",
		5: "빗방울",
		6: "빗방울·눈날림",
		7: "눈날림"
	}[p] ?? "강수";
	return {
		1: "맑음",
		3: "구름 많음",
		4: "흐림"
	}[Number(v.SKY)] ?? "하늘상태 확인 중";
}
function advice(data, now) {
	const notices = [];
	const current = data.observedAt && now.getTime() - Date.parse(data.observedAt) < 2 * 36e5 ? data.current : {};
	const hours = data.hours.filter((h) => Date.parse(h.at) >= now.getTime() && Date.parse(h.at) < now.getTime() + 24 * 36e5);
	const nextRain = hours.find((h) => (number(h.values.PTY) ?? 0) > 0 || (number(h.values.POP) ?? 0) >= 60);
	if (nextRain) notices.push({
		title: "우산을 준비하세요",
		body: `${new Intl.DateTimeFormat("ko-KR", {
			timeZone: "Asia/Seoul",
			month: "numeric",
			day: "numeric",
			hour: "numeric"
		}).format(new Date(nextRain.at))} ${condition(nextRain.values)} 또는 강수 가능성이 높게 예보되어 있습니다. 외출 전 최신 예보를 확인하세요.`,
		level: "watch"
	});
	const temps = hours.map((h) => number(h.values.TMP ?? h.values.T1H)).filter((v) => v !== null);
	const today = stamp(kst(now)), day = data.daily.find((d) => d.date === today);
	if (day?.min !== null && day?.max !== null && day && day.max - day.min >= 10) notices.push({
		title: "큰 일교차에 대비하세요",
		body: `오늘 예보 최저 ${day.min}° · 최고 ${day.max}°. 아침·저녁에는 겉옷을 준비하세요.`,
		level: "info"
	});
	if ([number(current.WSD), ...hours.map((h) => number(h.values.WSD))].filter((v) => v !== null).some((v) => v >= 9)) notices.push({
		title: "강한 바람에 주의하세요",
		body: "실황 또는 향후 24시간 예보에서 풍속 9m/s 이상이 확인됩니다. 야외 설치물을 점검하고 바람이 강한 곳에서 주의하세요.",
		level: "watch"
	});
	if (temps.some((t) => t >= 33) || (number(current.T1H) ?? -99) >= 33) notices.push({
		title: "더운 시간대 야외활동 주의",
		body: "기온 33°C 이상인 시간대가 있습니다. 야외활동 시 그늘에서 쉬고 물을 충분히 드세요.",
		level: "watch"
	});
	if (temps.some((t) => t <= 0) || (number(current.T1H) ?? 99) <= 0) notices.push({
		title: "영하 기온·미끄럼 주의",
		body: "영하 기온이 관측되거나 예보되어 있습니다. 젖은 도로와 그늘진 보행로가 얼 수 있으니 주의하세요.",
		level: "watch"
	});
	if ((number(current.RN1) ?? 0) >= 10) notices.push({
		title: "현재 강수량 증가",
		body: "1시간 강수량이 10mm 이상입니다. 하천변·지하차도 등은 현장 통제 안내를 확인하세요.",
		level: "watch"
	});
	if ((number(current.REH) ?? 100) <= 30) notices.push({
		title: "건조한 공기·화기 주의",
		body: "현재 상대습도가 30% 이하입니다. 산 주변에서 화기를 사용하지 말고 불씨를 주의하세요.",
		level: "info"
	});
	if (data.air && Date.parse(data.air.dataTime?.replace(" ", "T") + "+09:00") > now.getTime() - 3 * 36e5) {
		if (["3", "4"].includes(data.air.pm10Grade1h) || ["3", "4"].includes(data.air.pm25Grade1h)) notices.push({
			title: "미세먼지 나쁨 이상",
			body: "측정소에서 제공한 현재 미세먼지 등급이 나쁨 이상입니다. 어린이·어르신은 장시간 야외활동을 줄이세요.",
			level: "watch"
		});
		if (["3", "4"].includes(data.air.o3Grade)) notices.push({
			title: "오존 농도 주의",
			body: "측정소에서 제공한 오존 등급이 나쁨 이상입니다. 한낮의 장시간 야외활동을 줄이세요.",
			level: "watch"
		});
	}
	if (!notices.length) notices.push({
		title: hours.length ? "외출 전 최신 예보 확인" : "기상자료 연결 확인 중",
		body: hours.length ? "시간대별 기온과 강수확률을 확인하고 외출을 준비하세요. 지역·시간에 따라 실제 날씨는 달라질 수 있습니다." : "자료가 연결되면 예보에 따른 시민 안내가 자동으로 표시됩니다. 현재 날씨는 기상청 날씨누리를 확인하세요.",
		level: "info"
	});
	return notices;
}
//#endregion
//#region lib/air.ts
function airTimestamp(value) {
	if (!value) return null;
	const date = Date.parse(value.replace(" ", "T") + "+09:00");
	return Number.isFinite(date) ? date : null;
}
function issuedTimestamp(value) {
	const m = value.match(/(\d{4}-\d{2}-\d{2})\s+(\d{1,2})(?::(\d{2}))?/);
	return m ? Date.parse(`${m[1]}T${m[2].padStart(2, "0")}:${m[3] ?? "00"}:00+09:00`) : 0;
}
function metricValue(row, code) {
	if (!row || row[code + "Flag"]) return null;
	return number(row[code + "Value"]);
}
function airGrade(row, code) {
	if (metricValue(row, code) === null) return null;
	const value = row?.[code + (code === "pm10" || code === "pm25" ? "Grade1h" : "Grade")];
	return [
		"1",
		"2",
		"3",
		"4"
	].includes(String(value)) ? Number(value) : null;
}
function regionalGrade(text) {
	const pairs = text.split(/[,，]/).map((s) => s.trim());
	for (const name of [
		"경기북부",
		"경기",
		"경기도"
	]) {
		const pair = pairs.find((s) => s.split(":")[0]?.trim() === name);
		if (pair) return {
			region: name,
			grade: pair.slice(pair.indexOf(":") + 1).trim()
		};
	}
	return {
		region: "경기북부",
		grade: "자료 없음"
	};
}
function forecastDate(row) {
	const raw = row.informData ?? row.fcstDate ?? row.forecastDate ?? "";
	return raw.match(/\d{4}-\d{2}-\d{2}/)?.[0] ?? (/^\d{8}$/.test(raw) ? `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}` : "");
}
function normalizeForecasts(rows, today) {
	const map = /* @__PURE__ */ new Map();
	for (const row of rows) {
		const date = forecastDate(row);
		if (!date || date < today || ![
			"PM10",
			"PM25",
			"O3"
		].includes(row.informCode)) continue;
		const code = row.informCode, regional = regionalGrade(row.informGrade ?? "");
		const entry = {
			date,
			issuedAt: row.dataTime ?? "",
			code,
			...regional,
			overall: row.informOverall ?? row.infornOverall ?? "",
			cause: row.informCause ?? "",
			action: row.actionKnack ?? ""
		};
		const old = map.get(date + code);
		if (!old || issuedTimestamp(entry.issuedAt) > issuedTimestamp(old.issuedAt)) map.set(date + code, entry);
	}
	return [...map.values()].sort((a, b) => a.date.localeCompare(b.date) || a.code.localeCompare(b.code));
}
function normalizeHigh(rows, today) {
	const map = /* @__PURE__ */ new Map();
	for (const row of rows) {
		const date = forecastDate(row);
		if (!date || date < today) continue;
		const flag = (row.gyeonggibuk50Over ?? "").trim().toUpperCase();
		const entry = {
			date,
			issuedAt: row.dataTime ?? "",
			region: "경기북부",
			over50: flag === "O" ? true : flag === "X" || Object.prototype.hasOwnProperty.call(row, "gyeonggibuk50Over") && row.gyeonggibuk50Over === null ? false : null,
			grade: row.gyeonggibukGrade ?? "자료 없음",
			cause: row.informCause ?? row.informOverall ?? ""
		};
		const old = map.get(date);
		if (!old || issuedTimestamp(entry.issuedAt) > issuedTimestamp(old.issuedAt)) map.set(date, entry);
	}
	return [...map.values()].sort((a, b) => a.date.localeCompare(b.date));
}
function combinedAdvice(data, now) {
	const notices = [];
	const air = data.air;
	const airAt = airTimestamp(air?.dataTime);
	const usable = air && [
		"pm10",
		"pm25",
		"o3",
		"no2",
		"co",
		"so2"
	].some((code) => metricValue(air, code) !== null);
	const fresh = Boolean(usable) && airAt !== null && now.getTime() - airAt >= -36e5 && now.getTime() - airAt < 3 * 36e5;
	const weatherFresh = data.observedAt && now.getTime() - Date.parse(data.observedAt) < 2 * 36e5;
	const high = data.highForecasts?.filter((x) => x.over50 === true && issuedTimestamp(x.issuedAt) > now.getTime() - 36 * 36e5) ?? [];
	if (high.length) notices.push({
		title: "고농도 초미세먼지 전망에 대비하세요",
		body: `${high.map((x) => x.date.slice(5)).join("·")} 경기북부에서 초미세먼지 50㎍/㎥ 초과가 예보되었습니다. 민감군은 장시간 실외활동을 줄이고, 야외 일정은 최신 예보를 확인해 조정하세요. 비상저감조치 발령 여부는 별도 공지를 확인하세요.`,
		level: "watch"
	});
	const badNow = fresh && ["pm10", "pm25"].some((code) => (airGrade(air, code) ?? 0) >= 3);
	const badOzone = fresh && (airGrade(air, "o3") ?? 0) >= 3;
	if (badNow) notices.push({
		title: "미세먼지·야외활동 주의",
		body: `${air?.stationName} 측정소의 현재 먼지 등급이 나쁨 이상입니다. 어린이·어르신·호흡기 민감군은 장시간 실외활동을 줄이고, 환기는 최신 농도를 확인해 짧게 하세요.`,
		level: "watch"
	});
	if (badOzone) notices.push({
		title: "오존이 높은 시간대 활동 조정",
		body: `${air?.stationName} 측정소의 현재 오존 등급이 나쁨 이상입니다. 한낮의 장시간 야외활동을 줄이고 실내에서 쉬세요.`,
		level: "watch"
	});
	const badFuture = data.airForecasts?.filter((x) => [
		"나쁨",
		"매우나쁨",
		"매우 나쁨"
	].includes(x.grade) && issuedTimestamp(x.issuedAt) > now.getTime() - 36 * 36e5) ?? [];
	if (badFuture.length) notices.push({
		title: "앞으로의 대기질을 확인하세요",
		body: `${badFuture.slice(0, 3).map((x) => `${x.date.slice(5)} ${x.code === "PM25" ? "초미세먼지" : x.code === "PM10" ? "미세먼지" : "오존"} ${x.grade}`).join(" · ")} (${badFuture[0].region} 권역 예보). 야외 행사·운동 계획은 당일 측정값도 함께 확인하세요.`,
		level: "watch"
	});
	if (fresh && weatherFresh) {
		const pm25 = metricValue(air, "pm25"), temp = number(data.current.T1H), wind = number(data.current.WSD), rain = number(data.current.RN1);
		const facts = [
			temp === null ? null : `기온 ${temp}°C`,
			wind === null ? null : `풍속 ${wind}m/s`,
			pm25 === null ? "초미세먼지 측정자료 없음" : `초미세먼지 ${pm25}㎍/㎥`
		].filter(Boolean).join(" · ");
		const nextRain = data.hours.find((x) => Date.parse(x.at) >= now.getTime() && Date.parse(x.at) < now.getTime() + 12 * 36e5 && ((number(x.values.PTY) ?? 0) > 0 || (number(x.values.POP) ?? 0) >= 60));
		const text = badNow || badOzone ? "대기질을 우선 고려해 야외활동 시간을 줄이세요." : rain !== null && rain > 0 || nextRain ? "대기질과 함께 강수 예보도 확인하고, 우산과 미끄럼에 대비하세요." : wind !== null && wind >= 9 ? "대기질과 함께 강한 바람에도 대비해 야외 설치물과 이동 경로를 확인하세요." : "외출 전 시간대별 기온·강수와 최신 대기질을 함께 확인하세요.";
		notices.push({
			title: "날씨·대기질 종합 생활안내",
			body: `${facts}. ${text} 대기질은 ${air?.stationName} 측정소 기준입니다.`,
			level: badNow || badOzone ? "watch" : "info"
		});
	}
	const pmForecast = data.airForecasts?.find((f) => f.code === "PM25" && f.date === new Intl.DateTimeFormat("en-CA", {
		timeZone: "Asia/Seoul",
		year: "numeric",
		month: "2-digit",
		day: "2-digit"
	}).format(now) && issuedTimestamp(f.issuedAt) > now.getTime() - 36 * 36e5);
	if (pmForecast && !badFuture.length) notices.push({
		title: "오늘의 날씨·대기질 전망",
		body: `${pmForecast.region}의 오늘 초미세먼지 예보는 '${pmForecast.grade}'입니다. ${data.notices.find((n) => n.title === "큰 일교차에 대비하세요")?.body ?? "외출 전 시간대별 기온과 강수확률을 확인하세요."} 권역 예보와 현재 측정농도는 서로 다를 수 있습니다.`,
		level: "info"
	});
	if (!fresh) notices.push({
		title: "대기질 최신 자료 확인 필요",
		body: air && !usable ? "측정소의 통신장애·점검 또는 결측으로 현재 대기질 수치를 확인할 수 없습니다. 측정값을 정상·안전으로 판단하지 않고 권역 예보와 날씨를 함께 확인하세요." : air ? "대기측정 자료가 3시간 이상 지연되었거나 시각을 확인할 수 없어 최신 생활안내 분석에서 제외했습니다." : "현재 대기측정 자료를 확인하지 못했습니다. 날씨 안내와 함께 에어코리아의 최신 대기질도 확인하세요.",
		level: "info"
	});
	notices.push(...data.notices.filter((n) => !["미세먼지 나쁨 이상", "오존 농도 주의"].includes(n.title)));
	return notices;
}
//#endregion
//#region lib/weather-collector.ts
var memo = /* @__PURE__ */ new Map();
var inflight = /* @__PURE__ */ new Map();
function key(raw) {
	try {
		return decodeURIComponent(raw);
	} catch {
		return raw;
	}
}
async function getJSON(url, cacheKey, ttl, allowEmpty = false) {
	const cached = memo.get(cacheKey);
	if (cached && cached.until > Date.now()) return cached.value;
	if (inflight.has(cacheKey)) return inflight.get(cacheKey);
	const work = (async () => {
		const stored = await readApiCache(cacheKey);
		if (stored !== void 0) return stored;
		let cache;
		try {
			cache = typeof caches !== "undefined" ? caches.default : void 0;
		} catch {
			cache = void 0;
		}
		const request = new Request("https://weather-cache.invalid/" + cacheKey);
		if (cache) try {
			const r = await cache.match(request);
			if (r) return await r.json();
		} catch {
			cache = void 0;
		}
		let r;
		try {
			r = await fetch(url, { signal: AbortSignal.timeout(6e3) });
		} catch {
			throw new Error("기상자료 제공기관에 연결할 수 없습니다.");
		}
		if (!r.ok) throw new Error("자료 제공기관 응답 지연");
		const text = await r.text();
		let parsed;
		try {
			parsed = JSON.parse(text);
		} catch {
			const code = text.match(/<returnReasonCode>([^<]*)</)?.[1];
			throw new Error(code === "30" ? "API 활용승인 또는 인증키를 확인해 주세요." : "제공기관이 자료를 반환하지 않았습니다. 잠시 후 다시 확인합니다.");
		}
		const header = parsed.response?.header;
		if (!header || !["00", "0"].includes(String(header.resultCode))) throw new Error([
			"30",
			"31",
			"32"
		].includes(String(header?.resultCode)) ? "API 활용승인 또는 인증키를 확인해 주세요." : `제공기관 자료 준비 중 (${header?.resultCode ?? "응답 오류"})`);
		const rawItems = parsed.response?.body?.items?.item ?? parsed.response?.body?.items;
		const items = Array.isArray(rawItems) ? rawItems : rawItems && typeof rawItems === "object" ? [rawItems] : [];
		if (!allowEmpty && !items.length) throw new Error("해당 발표시각의 자료가 아직 없습니다.");
		await writeApiCache(cacheKey, items, ttl);
		memo.set(cacheKey, {
			until: Date.now() + ttl * 1e3,
			value: items
		});
		if (cache) try {
			await cache.put(request, new Response(JSON.stringify(items), { headers: {
				"Content-Type": "application/json",
				"Cache-Control": `public, max-age=${ttl}`
			} }));
		} catch {}
		return items;
	})();
	inflight.set(cacheKey, work);
	try {
		return await work;
	} finally {
		inflight.delete(cacheKey);
	}
}
async function weather(kind, now, raw, grid) {
	let message = "자료 연결 확인 중";
	if (!raw) return {
		items: [],
		source: {
			ok: false,
			message: "기상청 인증키 설정이 필요합니다.",
			base: null
		}
	};
	for (let back = 0; back < 2; back++) {
		const base = baseFor(kind, now, back);
		const url = new URL("https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/" + kind);
		for (const [k, v] of Object.entries({
			serviceKey: key(raw),
			pageNo: "1",
			numOfRows: kind === "getVilageFcst" ? "1500" : "1000",
			dataType: "JSON",
			base_date: base.date,
			base_time: base.time,
			nx: String(grid.nx),
			ny: String(grid.ny)
		})) url.searchParams.set(k, v);
		try {
			return {
				items: await getJSON(url, `${kind}-${base.date}-${base.time}-${grid.nx}-${grid.ny}`, 86400),
				source: {
					ok: true,
					message: "연결됨",
					base: at(base.date, base.time)
				}
			};
		} catch (e) {
			message = e instanceof Error ? e.message : message;
			if (message.includes("인증키")) break;
		}
	}
	return {
		items: [],
		source: {
			ok: false,
			message,
			base: null
		}
	};
}
async function collectWeather(grid) {
	const now = /* @__PURE__ */ new Date();
	const bindings = env;
	const day = stamp(kst(now)), searchDate = `${day.slice(0, 4)}-${day.slice(4, 6)}-${day.slice(6, 8)}`;
	const station = stationForRegion(grid);
	const airKey = bindings.AIRKOREA_API_KEY ?? "";
	const ttl = Math.max(60, Math.ceil((36e5 - Date.now() % 36e5) / 1e3));
	async function airCall(path, params, cacheKey) {
		if (!airKey) return {
			items: [],
			source: {
				ok: false,
				message: "대기질 인증키 설정 필요",
				base: null
			}
		};
		try {
			const url = new URL("https://apis.data.go.kr/B552584/" + path);
			for (const [k, v] of Object.entries({
				serviceKey: key(airKey),
				returnType: "json",
				pageNo: "1",
				numOfRows: "100",
				...params
			})) url.searchParams.set(k, v);
			const items = await getJSON(url, cacheKey, ttl, true);
			const base = items.map((x) => x.dataTime ?? "").sort((a, b) => issuedTimestamp(b) - issuedTimestamp(a))[0] ?? null;
			return {
				items,
				source: {
					ok: true,
					message: items.length ? "연결됨" : "해당 날짜의 발표 자료 없음",
					base
				}
			};
		} catch (e) {
			return {
				items: [],
				source: {
					ok: false,
					message: e instanceof Error ? e.message : "대기질 연결 확인 중",
					base: null
				}
			};
		}
	}
	const [current, ultra, forecast, airResult, airForecastResult, highResult] = await Promise.all([
		weather("getUltraSrtNcst", now, bindings.KMA_API_KEY ?? "", grid),
		weather("getUltraSrtFcst", now, bindings.KMA_API_KEY ?? "", grid),
		weather("getVilageFcst", now, bindings.KMA_API_KEY ?? "", grid),
		airCall("ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty", {
			stationName: station,
			dataTerm: "DAILY",
			ver: "1.3",
			numOfRows: "24"
		}, `air-current-${encodeURIComponent(station)}`),
		airCall("ArpltnInforInqireSvc/getMinuDustFrcstDspth", { searchDate }, `air-forecast-${searchDate}`),
		airCall("MinuDustFrcstDspthSvc/getMinuDustFrcstDspth50Over", {
			searchDate,
			informCode: "PM25"
		}, `air-high-${searchDate}`)
	]);
	let activeStation = station;
	let activeAirResult = airResult;
	let fallbackReason = "";
	const records = (rows, name) => rows.slice().sort((a, b) => String(b.dataTime).localeCompare(String(a.dataTime))).map((x) => ({
		...x,
		stationName: name
	}));
	const usable = (rows) => {
		const row = rows[0];
		const date = airTimestamp(row?.dataTime);
		return date !== null && now.getTime() - date < 3 * 36e5 && ["pm10", "pm25"].some((code) => metricValue(row, code) !== null);
	};
	if (!usable(records(airResult.items, station)) && airKey) {
		const alternatives = [
			"의정부1동",
			"의정부동",
			"송산3동"
		].filter((s) => s !== station);
		const found = (await Promise.all(alternatives.map(async (name) => ({
			name,
			result: await airCall("ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty", {
				stationName: name,
				dataTerm: "DAILY",
				ver: "1.3",
				numOfRows: "24"
			}, `air-current-${encodeURIComponent(name)}`)
		})))).find((x) => usable(records(x.result.items, x.name)));
		if (found) {
			activeStation = found.name;
			activeAirResult = found.result;
			fallbackReason = `${station} 측정자료 없음 → ${activeStation} 대체`;
			activeAirResult.source.message = `연결됨 · ${fallbackReason}`;
		}
	}
	const airHistory = records(activeAirResult.items, activeStation);
	if (!airHistory.length && activeAirResult.source.ok) {
		activeAirResult.source.ok = false;
		activeAirResult.source.message = "측정소 자료 없음";
	}
	if (airHistory.length) activeAirResult.source.base = airHistory[0].dataTime;
	const today = stamp(kst(now));
	let dailyItems = forecast.items;
	if (forecast.source.ok && !forecast.items.some((x) => x.fcstDate === today && x.category === "TMN") && kst(now).getUTCHours() >= 3) {
		const dawn = await weather("getVilageFcst", new Date(at(today, "0230")), bindings.KMA_API_KEY ?? "", grid);
		if (dawn.source.ok) dailyItems = [...dawn.items.filter((x) => x.fcstDate === today && ["TMN", "TMX"].includes(x.category) && !forecast.items.some((v) => v.fcstDate === today && v.category === x.category)), ...forecast.items];
	}
	const values = {};
	for (const x of current.items) values[x.category] = String(x.obsrValue);
	const grouped = /* @__PURE__ */ new Map();
	for (const [items, source] of [[forecast.items, "단기예보"], [ultra.items, "초단기예보"]]) for (const x of items) {
		const time = at(x.fcstDate, x.fcstTime);
		const h = grouped.get(time) ?? {
			at: time,
			values: {},
			source
		};
		h.values[x.category] = String(x.fcstValue);
		h.source = source;
		grouped.set(time, h);
	}
	const all = [...grouped.values()].sort((a, b) => a.at.localeCompare(b.at));
	const daily = [];
	for (const date of [...new Set(dailyItems.map((x) => x.fcstDate))].sort()) {
		const rows = dailyItems.filter((x) => x.fcstDate === date);
		const select = (c) => rows.filter((x) => x.category === c).map((x) => number(x.fcstValue)).filter((v) => v !== null);
		const mins = select("TMN"), maxs = select("TMX"), pops = select("POP");
		daily.push({
			date,
			min: mins.length ? Math.min(...mins) : null,
			max: maxs.length ? Math.max(...maxs) : null,
			pop: pops.length ? Math.max(...pops) : null
		});
	}
	const data = {
		fetchedAt: now.toISOString(),
		grid,
		current: values,
		observedAt: current.source.base,
		hours: all.filter((h) => Date.parse(h.at) >= now.getTime() - 36e5),
		daily: daily.filter((d) => d.date >= stamp(kst(now))),
		sources: {
			current: current.source,
			ultra: ultra.source,
			forecast: forecast.source,
			air: activeAirResult.source,
			airForecast: airForecastResult.source,
			high: highResult.source
		},
		air: airHistory[0] ?? null,
		airHistory,
		airForecasts: normalizeForecasts(airForecastResult.items, searchDate),
		highForecasts: normalizeHigh(highResult.items, searchDate),
		airStation: {
			name: activeStation,
			...AIR_STATIONS[activeStation],
			requestedName: station,
			fallbackReason
		},
		notices: []
	};
	data.notices = advice(data, now);
	data.notices = combinedAdvice(data, now);
	return data;
}
//#endregion
//#region lib/snapshot-merge.ts
var timestamp = (value) => Number.isFinite(Date.parse(value)) ? Date.parse(value) : issuedTimestamp(value);
var keys = [
	"current",
	"ultra",
	"forecast",
	"air",
	"airForecast",
	"high"
];
function mergeSnapshot(previous, incoming, now) {
	if (incoming.sources.air.ok && ![
		"pm10",
		"pm25",
		"o3",
		"no2",
		"co",
		"so2"
	].some((code) => metricValue(incoming.air, code) !== null)) incoming.sources.air = {
		...incoming.sources.air,
		ok: false,
		message: "측정자료 없음 · 마지막 저장 자료 보존"
	};
	if (!previous && !keys.some((k) => incoming.sources[k]?.ok)) return null;
	const data = previous ? structuredClone(previous) : structuredClone(incoming);
	const replace = (k) => {
		const next = incoming.sources[k];
		const old = previous?.sources[k];
		return Boolean(next?.ok && (!old?.base || !next.base || timestamp(next.base) >= timestamp(old.base)));
	};
	if (replace("current")) {
		data.current = incoming.current;
		data.observedAt = incoming.observedAt;
	}
	if (replace("forecast")) data.daily = incoming.daily;
	if (previous) {
		const hours = new Map(previous.hours.map((h) => [h.at, structuredClone(h)]));
		for (const h of incoming.hours) {
			const old = hours.get(h.at) ?? {
				...h,
				values: {}
			};
			for (const [code, value] of Object.entries(h.values)) if (replace([
				"T1H",
				"RN1",
				"REH",
				"PTY",
				"WSD",
				"VEC",
				"UUU",
				"VVV",
				"SKY"
			].includes(code) && incoming.sources.ultra.ok ? "ultra" : "forecast")) old.values[code] = value;
			hours.set(h.at, old);
		}
		data.hours = [...hours.values()].filter((h) => Date.parse(h.at) >= now.getTime() - 36e5).sort((a, b) => a.at.localeCompare(b.at));
	}
	if (replace("air") && incoming.air) {
		data.air = incoming.air;
		data.airHistory = incoming.airHistory;
		data.airStation = incoming.airStation;
	}
	if (replace("airForecast") && incoming.airForecasts?.length) data.airForecasts = incoming.airForecasts;
	if (replace("high") && incoming.highForecasts?.length) data.highForecasts = incoming.highForecasts;
	for (const k of keys) if (replace(k)) data.sources[k] = incoming.sources[k];
	data.fetchedAt = keys.some(replace) ? incoming.fetchedAt : previous?.fetchedAt ?? incoming.fetchedAt;
	data.storage = {
		savedAt: keys.some(replace) ? now.toISOString() : previous?.storage?.savedAt ?? data.fetchedAt,
		failedSources: keys.filter((k) => !incoming.sources[k]?.ok)
	};
	data.notices = advice(data, now);
	data.notices = combinedAdvice(data, now);
	return data;
}
//#endregion
//#region lib/weather-store.ts
function db$3() {
	if (!env.DB) throw new Error("자료 저장소 연결을 확인해 주세요.");
	return env.DB;
}
async function readSnapshot$1(id) {
	const row = await db$3().prepare("SELECT payload FROM weather_snapshots WHERE region_id = ?").bind(id).first();
	return row ? JSON.parse(row.payload) : null;
}
async function collectAndSave() {
	const started = Date.now();
	const lease = started + 9e4;
	if (!(await db$3().prepare("INSERT INTO collection_state (id,next_due,lease_until,last_started,last_finished,last_error) VALUES ('weather',0,?,?,NULL,NULL) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND collection_state.next_due<=?").bind(lease, started, started, started).run()).meta.changes) return { started: false };
	const failures = [];
	try {
		(await Promise.allSettled(REGIONS.filter((r) => r.id === "city").map(async (region) => {
			const data = mergeSnapshot(await readSnapshot$1(region.id), await collectWeather(region), /* @__PURE__ */ new Date());
			if (!data) throw new Error("사용 가능한 수집 자료 없음");
			if (data.storage?.failedSources.length) failures.push(region.name);
			await db$3().prepare("INSERT INTO weather_snapshots (region_id,payload,saved_at) VALUES (?,?,?) ON CONFLICT(region_id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(region.id, JSON.stringify(data), Date.parse(data.storage?.savedAt ?? data.fetchedAt)).run();
		}))).forEach((result, i) => {
			if (result.status === "rejected") failures.push("의정부시");
		});
	} finally {
		const now = Date.now();
		const previous = await readApiCache("weather-retry-v2");
		const attempt = failures.length ? (previous?.attempt ?? 0) + 1 : 0;
		await writeApiCache("weather-retry-v2", { attempt }, 86400);
		const nextDue = failures.length ? now + retryDelay(attempt - 1) : (Math.floor(now / 36e5) + 1) * 36e5;
		await db$3().prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id='weather' AND last_started=?").bind(nextDue, now, failures.length ? "일부 자료 수집 지연 · 마지막 저장 자료 표시" : null, started).run();
	}
	return {
		started: true,
		regions: 1,
		failedRegions: [...new Set(failures)]
	};
}
//#endregion
//#region lib/population.ts
var POP_REGIONS = [{
	id: "city",
	name: "의정부시 전체"
}, ...REGIONS.filter((r) => r.id !== "city")];
var emptyPopulation = () => ({
	population: [],
	migration: [],
	collectedAt: null,
	populationSavedAt: null,
	migrationSavedAt: null,
	pending: false,
	errors: []
});
function numeric$1(v) {
	if (v === null || v === void 0 || String(v).trim() === "" || String(v) === "-") return null;
	const n = Number(String(v).replaceAll(",", ""));
	return Number.isFinite(n) && n >= 0 ? n : null;
}
function value(row, map, name) {
	return row[map[name] ?? name];
}
function str(row, map, name) {
	return String(value(row, map, name) ?? "").trim();
}
function num(row, map, name) {
	return numeric$1(value(row, map, name));
}
function monthOffset(month, offset) {
	const d = new Date(Date.UTC(Number(month.slice(0, 4)), Number(month.slice(4, 6)) - 1 + offset, 1));
	return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}
function recentMonths(now = /* @__PURE__ */ new Date(), count = 3) {
	const d = new Date(now.getTime() + 9 * 36e5);
	const month = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
	return Array.from({ length: count }, (_, i) => monthOffset(month, -i - 1));
}
function normalizePopulation(rows, map) {
	const months = /* @__PURE__ */ new Map();
	const seen = /* @__PURE__ */ new Map();
	for (const row of rows) {
		const month = str(row, map, "month").replace(/\D/g, "").slice(0, 6);
		if (!/^\d{6}$/.test(month)) throw Error("통계 기준월 확인 필요");
		const city = str(row, map, "city");
		const code = str(row, map, "code");
		if (city && city !== "의정부시") continue;
		const name = str(row, map, "dong");
		const region = POP_REGIONS.find((r) => r.id === code || r.name === name) || (code === "4115000000" || !name && city === "의정부시" ? POP_REGIONS[0] : void 0);
		if (!region) continue;
		const population = num(row, map, "population"), households = num(row, map, "households"), male = num(row, map, "male"), female = num(row, map, "female");
		if ([
			population,
			households,
			male,
			female
		].some((n) => n === null) || male + female !== population) throw Error("인구·세대 수치 검증 필요");
		const tong = str(row, map, "tong"), ban = str(row, map, "ban");
		const key = [
			month,
			region.id,
			tong,
			ban
		].join("|");
		const body = JSON.stringify([
			population,
			households,
			male,
			female
		]);
		if (seen.has(key)) {
			if (seen.get(key) !== body) throw Error("중복 통반 자료 검증 필요");
			continue;
		}
		seen.set(key, body);
		if (!months.has(month)) months.set(month, /* @__PURE__ */ new Map());
		const group = months.get(month);
		if (!group.has(region.id)) group.set(region.id, []);
		group.get(region.id).push({
			tong,
			ban,
			population,
			households,
			male,
			female
		});
	}
	return [...months].map(([month, groups]) => {
		const regions = [];
		for (const [id, areas] of groups) {
			const detailed = areas.filter((a) => a.tong && !/^(0+|계|합계|전체)$/.test(a.tong));
			let chosen = areas;
			if (detailed.length) {
				chosen = [];
				for (const tong of new Set(detailed.map((a) => a.tong))) {
					const rows = detailed.filter((a) => a.tong === tong);
					const bans = rows.filter((a) => a.ban && !/^(0+|계|합계|전체)$/.test(a.ban));
					chosen.push(...bans.length ? bans : rows.slice(0, 1));
				}
			} else chosen = areas.slice(0, 1);
			const sums = chosen.reduce((s, a) => ({
				population: s.population + a.population,
				households: s.households + a.households,
				male: s.male + a.male,
				female: s.female + a.female
			}), {
				population: 0,
				households: 0,
				male: 0,
				female: 0
			});
			regions.push({
				id,
				name: POP_REGIONS.find((r) => r.id === id).name,
				...sums,
				tongCount: new Set(detailed.map((a) => a.tong)).size,
				banCount: chosen.filter((a) => a.ban && !/^(0+|계|합계|전체)$/.test(a.ban)).length,
				areas: chosen
			});
		}
		const dongs = regions.filter((r) => r.id !== "city");
		const complete = dongs.length === 15;
		const official = regions.find((r) => r.id === "city");
		const notes = [];
		if (!official && complete) regions.unshift({
			id: "city",
			name: "의정부시 전체",
			...dongs.reduce((s, r) => ({
				population: s.population + r.population,
				households: s.households + r.households,
				male: s.male + r.male,
				female: s.female + r.female,
				tongCount: s.tongCount + r.tongCount,
				banCount: s.banCount + r.banCount
			}), {
				population: 0,
				households: 0,
				male: 0,
				female: 0,
				tongCount: 0,
				banCount: 0
			}),
			areas: []
		});
		if (official && complete && dongs.reduce((s, r) => s + r.population, 0) !== official.population) notes.push("시 전체와 동별 합계 차이 확인 필요");
		if (!complete) notes.push(`동별 자료 ${dongs.length}/15개 확보 · 누락 동은 0명으로 표시하지 않음`);
		return {
			month,
			regions,
			complete,
			notes
		};
	}).sort((a, b) => b.month.localeCompare(a.month));
}
var AGE_GROUPS = [
	{
		label: "0–14세",
		from: 0,
		to: 14
	},
	{
		label: "15–19세",
		from: 15,
		to: 19
	},
	{
		label: "20–39세",
		from: 20,
		to: 39
	},
	{
		label: "40–64세",
		from: 40,
		to: 64
	},
	{
		label: "65세 이상",
		from: 65,
		to: 110
	}
];
function compactFlows(rows, map) {
	return rows.map((row) => {
		const month = str(row, map, "month").replace(/\D/g, "").slice(0, 6), from = str(row, map, "fromCode"), to = str(row, map, "toCode");
		const count = num(row, map, "population"), male = num(row, map, "male"), female = num(row, map, "female");
		if (!/^\d{6}$/.test(month) || !from || !to || [
			count,
			male,
			female
		].some((n) => n === null) || male + female !== count) throw Error("인구이동 수치·행정기관 확인 필요");
		let ageComplete = true;
		const ages = AGE_GROUPS.map((group) => {
			let sum = 0;
			for (let age = group.from; age <= group.to; age++) for (const sex of ["m", "f"]) {
				const n = num(row, map, `age:${sex}:${age}`);
				if (n === null) ageComplete = false;
				else sum += n;
			}
			return {
				label: group.label,
				count: sum
			};
		});
		if (ageComplete && ages.reduce((s, a) => s + a.count, 0) !== count) ageComplete = false;
		const name = (side) => [
			str(row, map, `${side}Province`),
			str(row, map, `${side}City`),
			str(row, map, `${side}Dong`)
		].filter(Boolean).join(" ");
		return {
			month,
			from,
			to,
			fromName: name("from") || from,
			toName: name("to") || to,
			count,
			male,
			female,
			ages,
			ageComplete
		};
	});
}
function summarizeFlows(flows) {
	if (!flows.length) throw Error("의정부시 이동 자료 확인 필요 · 마지막 성공 자료 유지");
	const unique = /* @__PURE__ */ new Map();
	for (const f of flows) {
		const key = [
			f.month,
			f.from,
			f.to
		].join("|");
		const old = unique.get(key);
		if (old && JSON.stringify(old) !== JSON.stringify(f)) throw Error("중복 인구이동 자료 검증 필요");
		unique.set(key, f);
	}
	return [...new Set([...unique.values()].map((f) => f.month))].map((month) => {
		const source = [...unique.values()].filter((f) => f.month === month);
		return {
			month,
			regions: POP_REGIONS.map((region) => {
				const origins = /* @__PURE__ */ new Map(), destinations = /* @__PURE__ */ new Map();
				const r = {
					id: region.id,
					inflow: 0,
					outflow: 0,
					internalIn: 0,
					internalOut: 0,
					maleIn: 0,
					maleOut: 0,
					femaleIn: 0,
					femaleOut: 0,
					ages: AGE_GROUPS.map((g) => ({
						label: g.label,
						inflow: 0,
						outflow: 0
					})),
					origins: [],
					destinations: []
				};
				for (const f of source) {
					const inCity = f.to.startsWith("41150"), fromCity = f.from.startsWith("41150");
					const incoming = region.id === "city" ? inCity && !fromCity : f.to === region.id && f.from !== region.id;
					const outgoing = region.id === "city" ? fromCity && !inCity : f.from === region.id && f.to !== region.id;
					if (region.id === "city" && inCity && fromCity && f.to !== f.from) {
						r.internalIn += f.count;
						r.internalOut += f.count;
					}
					if (incoming) {
						r.inflow += f.count;
						r.maleIn += f.male;
						r.femaleIn += f.female;
						if (region.id !== "city" && fromCity) r.internalIn += f.count;
						origins.set(f.fromName, (origins.get(f.fromName) ?? 0) + f.count);
						f.ages.forEach((a, i) => r.ages[i].inflow += a.count);
					}
					if (outgoing) {
						r.outflow += f.count;
						r.maleOut += f.male;
						r.femaleOut += f.female;
						if (region.id !== "city" && inCity) r.internalOut += f.count;
						destinations.set(f.toName, (destinations.get(f.toName) ?? 0) + f.count);
						f.ages.forEach((a, i) => r.ages[i].outflow += a.count);
					}
				}
				const rank = (m) => [...m].map(([name, count]) => ({
					name,
					count
				})).sort((a, b) => b.count - a.count).slice(0, 8);
				r.origins = rank(origins);
				r.destinations = rank(destinations);
				return r;
			}),
			ageComplete: source.every((f) => f.ageComplete),
			complete: true
		};
	}).sort((a, b) => b.month.localeCompare(a.month));
}
//#endregion
//#region lib/population-store.ts
var definitions = {
	residentAge: {
		endpoint: "https://apis.data.go.kr/1741000/admmSexdAgePpltn/selectAdmmSexdAgePpltn",
		portal: "https://www.data.go.kr/data/15108072/openapi.do"
	},
	population: {
		endpoint: "https://apis.data.go.kr/1741000/admmPpltnHhStus/selectAdmmPpltnHhStus",
		portal: "https://www.data.go.kr/data/15108065/openapi.do"
	},
	migration: {
		endpoint: "https://apis.data.go.kr/1741000/ppltnDataStus/selectPpltnDataStus",
		portal: "https://www.data.go.kr/data/15108093/openapi.do"
	}
};
function db$2() {
	if (!env.DB) throw Error("인구 DB 연결 확인 필요");
	return env.DB;
}
function walk(value) {
	if (!value || typeof value !== "object") return [];
	if (Array.isArray(value)) return value.flatMap(walk);
	return [value, ...Object.values(value).flatMap(walk)];
}
function extractContract(html) {
	const match = html.match(/(?:const|let|var)\s+swaggerJson\s*=\s*`([\s\S]*?)`/);
	if (!match?.[1]) return null;
	let spec;
	try {
		spec = JSON.parse(match[1]);
	} catch {
		return null;
	}
	const params = walk(spec.paths).filter((v) => v.in === "query" && typeof v.name === "string").map((v) => ({
		name: v.name,
		description: String(v.description ?? ""),
		required: v.required
	}));
	const fields = {};
	for (const v of walk(spec.definitions ?? spec.components?.schemas)) for (const [name, field] of Object.entries(v.properties ?? {})) if (field?.description) fields[name] = String(field.description);
	return {
		params,
		fields
	};
}
async function contract(kind) {
	const key = `population-contract:${kind}:v1`;
	const saved = await readApiCache(key);
	if (saved) return saved;
	try {
		const response = await fetch(definitions[kind].portal, { signal: AbortSignal.timeout(8e3) });
		if (response.ok) {
			const html = await response.text();
			let data = extractContract(html);
			if (!data) {
				const url = html.match(/swaggerUrl\s*=\s*['"]([^'"]+)['"]/);
				if (url) {
					const target = new URL(url[1], definitions[kind].portal);
					if (target.hostname === "www.data.go.kr" || target.hostname === "apis.data.go.kr") {
						const spec = await fetch(target, { signal: AbortSignal.timeout(5e3) });
						if (spec.ok) data = extractContract("const swaggerJson = `" + JSON.stringify(await spec.json()) + "`");
					}
				}
			}
			if (data?.params.length) {
				await writeApiCache(key, data, 86400);
				return data;
			}
		}
	} catch {}
	return {
		params: [],
		fields: {}
	};
}
var aliases = {
	month: [
		"statsYm",
		"statsYymm",
		"stats_ym",
		"statYm"
	],
	code: [
		"admmCd",
		"admm_cd",
		"admmInstCd"
	],
	city: ["sggNm", "sgg_nm"],
	dong: [
		"dongNm",
		"admmNm",
		"admm_nm"
	],
	tong: ["tong", "tongNo"],
	ban: ["ban", "banNo"],
	population: [
		"totNmprCnt",
		"totPpltnCnt",
		"tot_ppltn_cnt",
		"totalPopulation"
	],
	households: ["hhCnt", "hh_cnt"],
	male: [
		"maleNmprCnt",
		"malePpltnCnt",
		"male_ppltn_cnt"
	],
	female: [
		"femlNmprCnt",
		"femlPpltnCnt",
		"feml_ppltn_cnt",
		"femalePpltnCnt"
	],
	fromCode: [
		"mvtAdmmCd",
		"mvotAdmmCd",
		"mvot_admm_cd"
	],
	toCode: ["mvinAdmmCd", "mvin_admm_cd"],
	fromProvince: ["mvtCtpvNm", "mvotCtpvNm"],
	fromCity: ["mvtSggNm", "mvotSggNm"],
	fromDong: ["mvtDongNm", "mvotAdmmNm"],
	toProvince: ["mvinCtpvNm"],
	toCity: ["mvinSggNm"],
	toDong: ["mvinDongNm", "mvinAdmmNm"]
};
function fieldMap(schema, example) {
	const fields = { ...schema.fields };
	const map = {};
	const clean = (s) => s.replace(/<[^>]*>/g, "").replace(/[\s()·\/]/g, "");
	for (const [name, descs] of Object.entries({
		month: ["통계년월"],
		code: ["행정기관코드"],
		city: ["시군구명"],
		dong: ["행정동명"],
		tong: ["통"],
		ban: ["반"],
		population: ["총인구수"],
		households: ["세대수"],
		male: ["남자인구수"],
		female: ["여자인구수"],
		fromCode: ["전출행정기관코드"],
		toCode: ["전입행정기관코드"],
		fromProvince: ["전출시도명"],
		fromCity: ["전출시군구명"],
		fromDong: ["전출행정동명"],
		toProvince: ["전입시도명"],
		toCity: ["전입시군구명"],
		toDong: ["전입행정동명"]
	})) {
		const exact = Object.entries(fields).find(([, d]) => descs.includes(clean(d)));
		const alias = aliases[name]?.find((a) => a in example) ?? Object.keys(example).find((k) => aliases[name]?.some((a) => a.toLowerCase() === k.toLowerCase()));
		if (exact) map[name] = Object.keys(example).find((k) => k.trim() === exact[0].trim()) ?? exact[0].trim();
		else if (alias) map[name] = alias;
	}
	for (const [name, description] of Object.entries(fields)) {
		const age = clean(description).match(/(?:만)?(\d{1,3})세(?:이상)?(남자|여자)/);
		if (age) map[`age:${age[2] === "남자" ? "m" : "f"}:${Number(age[1])}`] = Object.keys(example).find((k) => k.trim() === name.trim()) ?? name.trim();
	}
	for (const name of Object.keys(example)) {
		const age = name.trim().match(/^(male|feml)(\d{1,3})AgeNmprCnt$/);
		if (age && Number(age[2]) <= 110) map[`age:${age[1] === "male" ? "m" : "f"}:${Number(age[2])}`] = name;
	}
	return map;
}
function rowsAndCount(json) {
	const objects = walk(json);
	let rows = objects.find((o) => Array.isArray(o.row))?.row;
	if (!rows) {
		const v = objects.find((o) => o.items?.item)?.items?.item;
		rows = Array.isArray(v) ? v : v ? [v] : [];
	}
	const count = objects.map((o) => o.totalCount ?? o.list_total_count ?? o.total_count).find((v) => v !== void 0);
	const total = count === void 0 ? null : Number(count);
	const result = objects.find((o) => o.RESULT)?.RESULT ?? objects.find((o) => o.resultCode)?.resultCode;
	const code = typeof result === "object" ? result.CODE : result;
	if (code && ![
		"INFO-000",
		"00",
		"0"
	].includes(String(code))) throw Error(`제공기관 응답 코드 ${String(code).slice(0, 20)} · ${String(objects.find((o) => o.resultMsg)?.resultMsg ?? "").replace(/https?:\/\/\S+/g, "").replace(/[A-Za-z0-9+/%=]{40,}/g, "[비공개]").slice(0, 140)}`);
	if (!rows?.length) return {
		rows: [],
		total: total ?? 0
	};
	return {
		rows,
		total
	};
}
async function readPopulation() {
	const row = await db$2().prepare("SELECT payload FROM population_snapshot WHERE id='uijeongbu'").first();
	return row ? JSON.parse(row.payload) : null;
}
var countJob = (j) => j?.count ?? j?.items?.length ?? 0;
async function signatureHash(value) {
	return [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)))].map((n) => n.toString(16).padStart(2, "0")).join("");
}
async function migratePageJob(id, state) {
	if (!state.items) return state;
	const writes = [];
	for (let start = 0; start < state.items.length; start += 100) writes.push(db$2().prepare("INSERT INTO population_jobs (id,payload,saved_at) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(`${id}:page:${start / 100 + 1}`, JSON.stringify({ items: state.items.slice(start, start + 100) }), Date.now()));
	const result = {
		...state,
		count: state.items.length,
		items: void 0,
		signatures: await Promise.all(state.signatures.map(signatureHash))
	};
	writes.push(db$2().prepare("UPDATE population_jobs SET payload=?,saved_at=? WHERE id=?").bind(JSON.stringify(result), Date.now(), id));
	await db$2().batch(writes);
	return result;
}
async function job(id) {
	const r = await db$2().prepare("SELECT payload FROM population_jobs WHERE id=?").bind(id).first();
	return r ? JSON.parse(r.payload) : null;
}
async function saveJob$1(id, payload) {
	await db$2().prepare("INSERT INTO population_jobs (id,payload,saved_at) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(id, JSON.stringify(payload), Date.now()).run();
}
async function providerPage(kind, params, timeoutMs = 22e3) {
	let key = env.POPULATION_API_KEY ?? env.KMA_API_KEY;
	try {
		key = decodeURIComponent(key);
	} catch {}
	const url = new URL(definitions[kind].endpoint);
	for (const [k, v] of Object.entries({
		...params,
		serviceKey: key
	})) url.searchParams.set(k, v);
	const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
	if (!response.ok) throw Error("인구 제공기관 연결 지연");
	let json;
	try {
		json = await response.json();
	} catch {
		throw Error("인구 제공기관 응답 확인 필요");
	}
	const head = walk(json).find((o) => o.resultCode) ?? {};
	if (String(head.resultCode) === "3" && String(head.resultMsg) === "NODATA_ERROR") return {
		rows: [],
		total: 0
	};
	return rowsAndCount(json);
}
async function provinces(month) {
	const saved = await readApiCache("population-provinces:v1");
	if (saved?.length) return saved;
	const result = await providerPage("population", {
		type: "json",
		numOfRows: "100",
		pageNo: "1",
		lv: "1",
		admmCd: "0000000000",
		srchFrYm: month,
		srchToYm: month
	});
	const codes = [...new Set(result.rows.map((r) => String(r.admmCd ?? "")).filter((code) => /^\d{2}0{8}$/.test(code)))];
	if (codes.length < 15) throw Error("전국 시도 코드 조회 확인 필요 · 일부 시도를 전체로 집계하지 않음");
	await writeApiCache("population-provinces:v1", codes, 86400);
	return codes;
}
async function migrationStep(c, batch, monthList) {
	const tasks = (await provinces(monthList[0])).flatMap((code) => ["in", "out"].map((side) => ({
		id: `${batch}:${side}:${code}`,
		side,
		code
	})));
	const pending = (await Promise.all(tasks.map(async (task) => ({
		task,
		state: await job(task.id)
	})))).filter((v) => !v.state?.done).sort((a, b) => (a.state?.page ?? 1) - (b.state?.page ?? 1));
	const errors = [];
	await Promise.all(pending.slice(0, 4).map(async ({ task, state }) => {
		try {
			const current = state ? await migratePageJob(task.id, state) : {
				page: 1,
				done: false,
				count: 0,
				signatures: [],
				total: null
			};
			const result = await providerPage("migration", {
				type: "json",
				numOfRows: "100",
				pageNo: String(current.page),
				lv: "3",
				mvinAdmmCd: task.side === "in" ? "4115000000" : task.code,
				mvtAdmmCd: task.side === "out" ? "4115000000" : task.code,
				srchFrYm: monthList[2],
				srchToYm: monthList[0]
			});
			if (result.total !== null) current.total = result.total;
			if (result.rows.length) {
				const signature = await signatureHash(JSON.stringify([result.rows[0], result.rows.at(-1)]));
				if (current.signatures.includes(signature)) throw Error("페이지 중복 응답 확인 필요");
				current.signatures.push(signature);
				const items = compactFlows(result.rows, fieldMap(c, result.rows[0]));
				await saveJob$1(`${task.id}:page:${current.page}`, { items });
				current.count = (current.count ?? 0) + items.length;
				current.page++;
			}
			current.done = current.total !== null ? countJob(current) >= current.total : result.rows.length < 100;
			if (!result.rows.length && !current.done) throw Error("이동 페이지 일부 수집 지연");
			if (current.page > 150) throw Error("이동 페이지 수집 범위 확인 필요");
			await saveJob$1(task.id, current);
		} catch {
			errors.push("인구이동 일부 페이지 수집 지연 · 수집 완료 전에는 합계를 확정하지 않음");
		}
	}));
	const finished = await Promise.all(tasks.map(async (task) => await job(task.id)));
	const done = finished.filter((j) => j?.done).length;
	return {
		done,
		total: tasks.length,
		rows: finished.reduce((n, j) => n + countJob(j), 0),
		totalRows: finished.reduce((n, j) => n + (j?.total ?? 0), 0),
		errors: [...new Set(errors)],
		months: done === tasks.length ? summarizeFlows((await Promise.all(finished.map(async (j, i) => {
			if (j?.items) return j.items;
			return (await Promise.all(Array.from({ length: Math.max(0, (j?.page ?? 1) - 1) }, (_, page) => job(`${tasks[i].id}:page:${page + 1}`)))).flatMap((page) => page?.items ?? []);
		}))).flat()) : null
	};
}
async function collectPopulation(force = false) {
	const started = Date.now();
	if (!(await db$2().prepare("INSERT INTO collection_state (id,next_due,lease_until,last_started,last_finished,last_error) VALUES ('population',0,?,?,NULL,NULL) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND (collection_state.next_due<=? OR ?=1)").bind(started + 45e3, started, started, started, force ? 1 : 0).run()).meta.changes) return { started: false };
	const data = {
		...await readPopulation() ?? emptyPopulation(),
		pending: false,
		errors: [],
		collectedAt: new Date(started).toISOString()
	};
	let ongoing = false;
	try {
		const missing = recentMonths(new Date(started), 5).filter((month) => !data.population.some((row) => row.month === month && row.complete));
		if (missing.length) {
			const schema = await contract("population");
			for (const month of missing.slice(0, 2)) {
				const result = await providerPage("population", {
					type: "json",
					numOfRows: "100",
					pageNo: "1",
					lv: "3",
					admmCd: "4115000000",
					srchFrYm: month,
					srchToYm: month
				});
				if (!result.rows.length) {
					data.errors.push("새 기준월의 인구 자료 공개 대기");
					continue;
				}
				if (result.total !== result.rows.length) throw Error("월별 인구 전체 자료 확인 필요");
				const parsed = normalizePopulation(result.rows, fieldMap(schema, result.rows[0]));
				if (parsed.length !== 1 || parsed[0].month !== month || !parsed[0].complete) throw Error("15개 동 전체 자료 확인 필요");
				data.population = [...data.population.filter((row) => row.month !== month), ...parsed].sort((a, b) => b.month.localeCompare(a.month));
				data.populationSavedAt = (/* @__PURE__ */ new Date()).toISOString();
			}
			ongoing = missing.length > 2;
		}
		const month = recentMonths(new Date(started)).find((month) => !data.migration.some((row) => row.month === month && row.complete));
		if (month) {
			const result = await migrationStep(await contract("migration"), monthlyBatch(month), [
				month,
				month,
				month
			]);
			data.progress = {
				done: result.done,
				total: result.total,
				phase: `${month} 전국 시도별 전입·전출 페이지 수집`,
				rows: result.rows,
				totalRows: result.totalRows
			};
			data.errors.push(...result.errors);
			if (result.months) {
				data.migration = [...data.migration.filter((row) => !result.months.some((next) => next.month === row.month)), ...result.months].sort((a, b) => b.month.localeCompare(a.month));
				data.migrationSavedAt = (/* @__PURE__ */ new Date()).toISOString();
				data.progress = void 0;
				ongoing = ongoing || recentMonths(new Date(started)).some((month) => !data.migration.some((row) => row.month === month && row.complete));
			} else ongoing = true;
		} else data.progress = void 0;
	} catch (e) {
		data.errors.push(e instanceof Error && !["AbortError", "TimeoutError"].includes(e.name) ? e.message : "제공기관 응답 지연 · 기존 성공 자료 유지");
	} finally {
		const finished = Date.now();
		const retryKey = "population-retry-v2";
		const retry = await readApiCache(retryKey);
		const attempt = data.errors.length ? (retry?.attempt ?? 0) + 1 : 0;
		await writeApiCache(retryKey, { attempt }, 7 * 86400);
		const due = data.errors.length ? finished + (data.errors.every((e) => e.includes("공개 대기")) ? DAY$2 : retryDelay(attempt - 1)) : ongoing ? finished + 1e3 : finished + DAY$2;
		data.pending = ongoing && !data.errors.length;
		await db$2().batch([db$2().prepare("INSERT INTO population_snapshot (id,payload,saved_at) VALUES ('uijeongbu',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(JSON.stringify(data), finished), db$2().prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id='population' AND last_started=?").bind(due, finished, data.errors.join(" · ") || null, started)]);
		await db$2().prepare("DELETE FROM population_jobs WHERE id GLOB 'population-??????-[0-9]*' AND saved_at<?").bind(finished - 2 * DAY$2).run();
	}
	return {
		started: true,
		pending: data.pending,
		populationMonths: data.population.map((m) => m.month),
		migrationMonths: data.migration.map((m) => m.month),
		progress: data.progress,
		errors: data.errors
	};
}
async function collectPopulationAreas(id, month) {
	if (!POP_REGIONS.some((r) => r.id === id && id !== "city") || !/^20\d{4}$/.test(month)) throw Error("지역과 통계 기준월 확인 필요");
	const key = `population-areas:${month}:${id}`, started = Date.now();
	const completed = await job(key);
	if (completed?.done && completed.summary) return { started: false };
	if (!(await db$2().prepare("INSERT INTO collection_state (id,next_due,lease_until,last_started,last_finished,last_error) VALUES (?,0,?,?,NULL,NULL) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND collection_state.next_due<=?").bind(key, started + 85e3, started, started, started).run()).meta.changes) return { started: false };
	const old = completed;
	const data = old && !old.done ? {
		...old,
		error: null
	} : {
		page: 1,
		done: false,
		rows: [],
		summary: old?.summary ?? null,
		savedAt: old?.savedAt ?? null,
		error: null,
		total: null
	};
	const checkpoint = {
		page: data.page,
		rows: [...data.rows],
		total: data.total
	};
	try {
		const c = await contract("population");
		const result = await providerPage("population", {
			type: "json",
			numOfRows: "100",
			pageNo: String(data.page),
			lv: "4",
			admmCd: id,
			srchFrYm: month,
			srchToYm: month
		}, 6e4);
		if (!result.rows.length) throw Error("통·반 자료 확인 필요");
		data.total = result.total;
		data.rows.push(...result.rows);
		data.page++;
		if (result.total !== null && data.rows.length >= result.total) {
			const fields = fieldMap(c, result.rows[0]);
			let parsed = normalizePopulation(data.rows, fields).find((m) => m.month === month)?.regions.find((r) => r.id === id);
			const stock = (await readPopulation())?.population.find((m) => m.month === month)?.regions.find((r) => r.id === id);
			if (parsed && stock && (parsed.population !== stock.population || parsed.households !== stock.households)) {
				const areas = data.rows.map((row) => ({
					tong: String(row[fields.tong] ?? "").trim(),
					ban: String(row[fields.ban] ?? "").trim(),
					population: numeric$1(row[fields.population]),
					households: numeric$1(row[fields.households]),
					male: numeric$1(row[fields.male]),
					female: numeric$1(row[fields.female])
				}));
				const sums = areas.reduce((a, r) => ({
					population: a.population + r.population,
					households: a.households + r.households,
					male: a.male + r.male,
					female: a.female + r.female
				}), {
					population: 0,
					households: 0,
					male: 0,
					female: 0
				});
				if (sums.population === stock.population && sums.households === stock.households && sums.male === stock.male && sums.female === stock.female) parsed = {
					...parsed,
					...sums,
					areas
				};
			}
			if (!parsed || !stock) throw Error("통·반 제공 범위 확인 필요");
			data.note = parsed.population === stock.population && parsed.households === stock.households ? null : `통·반 표의 제공 합계는 ${parsed.population.toLocaleString("ko-KR")}명·${parsed.households.toLocaleString("ko-KR")}세대입니다. 동 전체 ${stock.population.toLocaleString("ko-KR")}명·${stock.households.toLocaleString("ko-KR")}세대와의 차이는 ${stock.population - parsed.population}명·${stock.households - parsed.households}세대입니다. 상세 표의 제공 범위와 집계 차이를 확인해야 하며, 이 값으로 동 전체 수치를 대체하지 않습니다.`;
			data.summary = parsed;
			data.savedAt = (/* @__PURE__ */ new Date()).toISOString();
			data.done = true;
			data.rows = [];
		}
		if (data.page > 100) throw Error("통·반 수집 범위 확인 필요");
	} catch (e) {
		data.error = e instanceof Error && !["AbortError", "TimeoutError"].includes(e.name) ? e.message : "통·반 제공기관 응답 지연";
		data.page = checkpoint.page;
		data.rows = checkpoint.rows;
		data.total = checkpoint.total;
	}
	await saveJob$1(key, data);
	await db$2().prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id=? AND last_started=?").bind(data.error ? Date.now() + 15 * 6e4 : data.done ? Date.now() + 30 * DAY$2 : Date.now() + 1e3, Date.now(), data.error, key, started).run();
	return { started: true };
}
async function loadPopulationAreas(id, month) {
	if (!POP_REGIONS.some((r) => r.id === id && id !== "city") || !/^20\d{4}$/.test(month)) throw Error("지역과 통계 기준월 확인 필요");
	const key = `population-areas:${month}:${id}`;
	const [data, state] = await Promise.all([job(key), db$2().prepare("SELECT lease_until,next_due FROM collection_state WHERE id=?").bind(key).first()]);
	const pending = !data?.done && (!state || state.lease_until > Date.now() || state.next_due <= Date.now() || Boolean(data && !data.error));
	return {
		id,
		month,
		row: data?.summary ?? null,
		savedAt: data?.savedAt ?? null,
		pending,
		error: data?.error ?? null,
		note: data?.note ?? null
	};
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/util.js
var nameStartChar = ":A-Za-z_\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD";
nameStartChar + "";
var nameRegexp = "[" + nameStartChar + "][:A-Za-z_\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD\\-.\\d\\u00B7\\u0300-\\u036F\\u203F-\\u2040]*";
var regexName = new RegExp("^" + nameRegexp + "$");
function getAllMatches(string, regex) {
	const matches = [];
	let match = regex.exec(string);
	while (match) {
		const allmatches = [];
		allmatches.startIndex = regex.lastIndex - match[0].length;
		const len = match.length;
		for (let index = 0; index < len; index++) allmatches.push(match[index]);
		matches.push(allmatches);
		match = regex.exec(string);
	}
	return matches;
}
var isName = function(string) {
	const match = regexName.exec(string);
	return !(match === null || typeof match === "undefined");
};
function isExist(v) {
	return typeof v !== "undefined";
}
/**
* Dangerous property names that could lead to prototype pollution or security issues
*/
var DANGEROUS_PROPERTY_NAMES = [
	"hasOwnProperty",
	"toString",
	"valueOf",
	"__defineGetter__",
	"__defineSetter__",
	"__lookupGetter__",
	"__lookupSetter__"
];
var criticalProperties = [
	"__proto__",
	"constructor",
	"prototype"
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/validator.js
var defaultOptions$1 = {
	allowBooleanAttributes: false,
	unpairedTags: []
};
function validate(xmlData, options) {
	options = Object.assign({}, defaultOptions$1, options);
	const tags = [];
	let tagFound = false;
	let reachedRoot = false;
	if (xmlData[0] === "﻿") xmlData = xmlData.substr(1);
	for (let i = 0; i < xmlData.length; i++) if (xmlData[i] === "<" && xmlData[i + 1] === "?") {
		i += 2;
		i = readPI(xmlData, i);
		if (i.err) return i;
	} else if (xmlData[i] === "<") {
		let tagStartPos = i;
		i++;
		if (xmlData[i] === "!") {
			i = readCommentAndCDATA(xmlData, i);
			continue;
		} else {
			let closingTag = false;
			if (xmlData[i] === "/") {
				closingTag = true;
				i++;
			}
			let tagName = "";
			for (; i < xmlData.length && xmlData[i] !== ">" && xmlData[i] !== " " && xmlData[i] !== "	" && xmlData[i] !== "\n" && xmlData[i] !== "\r"; i++) tagName += xmlData[i];
			tagName = tagName.trim();
			if (tagName[tagName.length - 1] === "/") {
				tagName = tagName.substring(0, tagName.length - 1);
				i--;
			}
			if (!validateTagName(tagName)) {
				let msg;
				if (tagName.trim().length === 0) msg = "Invalid space after '<'.";
				else msg = "Tag '" + tagName + "' is an invalid name.";
				return getErrorObject("InvalidTag", msg, getLineNumberForPosition(xmlData, i));
			}
			const result = readAttributeStr(xmlData, i);
			if (result === false) return getErrorObject("InvalidAttr", "Attributes for '" + tagName + "' have open quote.", getLineNumberForPosition(xmlData, i));
			let attrStr = result.value;
			i = result.index;
			if (attrStr[attrStr.length - 1] === "/") {
				const attrStrStart = i - attrStr.length;
				attrStr = attrStr.substring(0, attrStr.length - 1);
				const isValid = validateAttributeString(attrStr, options);
				if (isValid === true) tagFound = true;
				else return getErrorObject(isValid.err.code, isValid.err.msg, getLineNumberForPosition(xmlData, attrStrStart + isValid.err.line));
			} else if (closingTag) if (!result.tagClosed) return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' doesn't have proper closing.", getLineNumberForPosition(xmlData, i));
			else if (attrStr.trim().length > 0) return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' can't have attributes or invalid starting.", getLineNumberForPosition(xmlData, tagStartPos));
			else if (tags.length === 0) return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' has not been opened.", getLineNumberForPosition(xmlData, tagStartPos));
			else {
				const otg = tags.pop();
				if (tagName !== otg.tagName) {
					let openPos = getLineNumberForPosition(xmlData, otg.tagStartPos);
					return getErrorObject("InvalidTag", "Expected closing tag '" + otg.tagName + "' (opened in line " + openPos.line + ", col " + openPos.col + ") instead of closing tag '" + tagName + "'.", getLineNumberForPosition(xmlData, tagStartPos));
				}
				if (tags.length == 0) reachedRoot = true;
			}
			else {
				const isValid = validateAttributeString(attrStr, options);
				if (isValid !== true) return getErrorObject(isValid.err.code, isValid.err.msg, getLineNumberForPosition(xmlData, i - attrStr.length + isValid.err.line));
				if (reachedRoot === true) return getErrorObject("InvalidXml", "Multiple possible root nodes found.", getLineNumberForPosition(xmlData, i));
				else if (options.unpairedTags.indexOf(tagName) !== -1) {} else tags.push({
					tagName,
					tagStartPos
				});
				tagFound = true;
			}
			for (i++; i < xmlData.length; i++) if (xmlData[i] === "<") if (xmlData[i + 1] === "!") {
				i++;
				i = readCommentAndCDATA(xmlData, i);
				continue;
			} else if (xmlData[i + 1] === "?") {
				i = readPI(xmlData, ++i);
				if (i.err) return i;
			} else break;
			else if (xmlData[i] === "&") {
				const afterAmp = validateAmpersand(xmlData, i);
				if (afterAmp == -1) return getErrorObject("InvalidChar", "char '&' is not expected.", getLineNumberForPosition(xmlData, i));
				i = afterAmp;
			} else if (reachedRoot === true && !isWhiteSpace(xmlData[i])) return getErrorObject("InvalidXml", "Extra text at the end", getLineNumberForPosition(xmlData, i));
			if (xmlData[i] === "<") i--;
		}
	} else {
		if (isWhiteSpace(xmlData[i])) continue;
		return getErrorObject("InvalidChar", "char '" + xmlData[i] + "' is not expected.", getLineNumberForPosition(xmlData, i));
	}
	if (!tagFound) return getErrorObject("InvalidXml", "Start tag expected.", 1);
	else if (tags.length == 1) return getErrorObject("InvalidTag", "Unclosed tag '" + tags[0].tagName + "'.", getLineNumberForPosition(xmlData, tags[0].tagStartPos));
	else if (tags.length > 0) return getErrorObject("InvalidXml", "Invalid '" + JSON.stringify(tags.map((t) => t.tagName), null, 4).replace(/\r?\n/g, "") + "' found.", {
		line: 1,
		col: 1
	});
	return true;
}
function isWhiteSpace(char) {
	return char === " " || char === "	" || char === "\n" || char === "\r";
}
/**
* Read Processing insstructions and skip
* @param {*} xmlData
* @param {*} i
*/
function readPI(xmlData, i) {
	const start = i;
	for (; i < xmlData.length; i++) if (xmlData[i] == "?" || xmlData[i] == " ") {
		const tagname = xmlData.substr(start, i - start);
		if (i > 5 && tagname === "xml") return getErrorObject("InvalidXml", "XML declaration allowed only at the start of the document.", getLineNumberForPosition(xmlData, i));
		else if (xmlData[i] == "?" && xmlData[i + 1] == ">") {
			i++;
			break;
		} else continue;
	}
	return i;
}
function readCommentAndCDATA(xmlData, i) {
	if (xmlData.length > i + 5 && xmlData[i + 1] === "-" && xmlData[i + 2] === "-") {
		for (i += 3; i < xmlData.length; i++) if (xmlData[i] === "-" && xmlData[i + 1] === "-" && xmlData[i + 2] === ">") {
			i += 2;
			break;
		}
	} else if (xmlData.length > i + 8 && xmlData[i + 1] === "D" && xmlData[i + 2] === "O" && xmlData[i + 3] === "C" && xmlData[i + 4] === "T" && xmlData[i + 5] === "Y" && xmlData[i + 6] === "P" && xmlData[i + 7] === "E") {
		let angleBracketsCount = 1;
		for (i += 8; i < xmlData.length; i++) if (xmlData[i] === "<") angleBracketsCount++;
		else if (xmlData[i] === ">") {
			angleBracketsCount--;
			if (angleBracketsCount === 0) break;
		}
	} else if (xmlData.length > i + 9 && xmlData[i + 1] === "[" && xmlData[i + 2] === "C" && xmlData[i + 3] === "D" && xmlData[i + 4] === "A" && xmlData[i + 5] === "T" && xmlData[i + 6] === "A" && xmlData[i + 7] === "[") {
		for (i += 8; i < xmlData.length; i++) if (xmlData[i] === "]" && xmlData[i + 1] === "]" && xmlData[i + 2] === ">") {
			i += 2;
			break;
		}
	}
	return i;
}
var doubleQuote = "\"";
var singleQuote = "'";
/**
* Keep reading xmlData until '<' is found outside the attribute value.
* @param {string} xmlData
* @param {number} i
*/
function readAttributeStr(xmlData, i) {
	let attrStr = "";
	let startChar = "";
	let tagClosed = false;
	for (; i < xmlData.length; i++) {
		if (xmlData[i] === doubleQuote || xmlData[i] === singleQuote) if (startChar === "") startChar = xmlData[i];
		else if (startChar !== xmlData[i]) {} else startChar = "";
		else if (xmlData[i] === ">") {
			if (startChar === "") {
				tagClosed = true;
				break;
			}
		}
		attrStr += xmlData[i];
	}
	if (startChar !== "") return false;
	return {
		value: attrStr,
		index: i,
		tagClosed
	};
}
/**
* Walk `attrStr` once, left to right, splitting it into attribute tokens.
*
* This replaces a regex that used to do the same job
* (`(\s*)([^\s=]+)(\s*=)?(\s*(['"])(([\s\S])*?)\5)?`). That regex led with an
* optional whitespace group followed by a required "non-whitespace" group.
* On a long run of whitespace that never resolves into an attribute name
* (e.g. a tag with thousands of trailing spaces before `>`), the engine
* backtracks the whitespace group one character at a time before giving up
* and moving to the next starting position — one full backtrack per
* position, which is quadratic in the length of the run.
*
* A single forward-only scan can never backtrack, so it can't be made slow
* this way no matter how much whitespace the input contains — it's always
* proportional to the length of the string, once.
*
* Each returned token mirrors the shape the old regex match array had, so
* the validation logic below (which reads token[1]..token[6]) didn't need
* to change:
*   token.startIndex - where this token begins in attrStr
*   token[1]          - leading whitespace before the name
*   token[2]          - the attribute name
*   token[3]          - whitespace + '=' if present, else undefined
*   token[4]          - marker (any defined value) if a quoted value was found
*   token[5]          - the quote character used ('"' or "'")
*   token[6]          - the value's text, without the surrounding quotes
*
* A malformed leading character (e.g. a stray '=' with no name before it)
* is simply skipped over, one character at a time — the same outcome the
* old regex produced by failing to match at that position and retrying at
* the next one.
*/
function scanAttributeTokens(attrStr) {
	const tokens = [];
	const len = attrStr.length;
	let i = 0;
	while (i < len) {
		const tokenStart = i;
		while (i < len && isWhiteSpace(attrStr[i])) i++;
		if (i >= len) break;
		if (attrStr[i] === "=") {
			i = tokenStart + 1;
			continue;
		}
		const leadingWs = attrStr.slice(tokenStart, i);
		const nameStart = i;
		while (i < len && !isWhiteSpace(attrStr[i]) && attrStr[i] !== "=") i++;
		const name = attrStr.slice(nameStart, i);
		let equalsGroup;
		let j = i;
		while (j < len && isWhiteSpace(attrStr[j])) j++;
		if (j < len && attrStr[j] === "=") {
			equalsGroup = attrStr.slice(i, j + 1);
			i = j + 1;
		}
		let quoteChar;
		let value;
		let k = i;
		while (k < len && isWhiteSpace(attrStr[k])) k++;
		if (k < len && (attrStr[k] === "\"" || attrStr[k] === "'")) {
			const valueStart = k + 1;
			const closeIdx = attrStr.indexOf(attrStr[k], valueStart);
			if (closeIdx !== -1) {
				quoteChar = attrStr[k];
				value = attrStr.slice(valueStart, closeIdx);
				i = closeIdx + 1;
			}
		}
		const token = { startIndex: tokenStart };
		token[1] = leadingWs;
		token[2] = name;
		token[3] = equalsGroup;
		token[4] = quoteChar !== void 0 ? true : void 0;
		token[5] = quoteChar;
		token[6] = value;
		tokens.push(token);
	}
	return tokens;
}
function validateAttributeString(attrStr, options) {
	const matches = scanAttributeTokens(attrStr);
	const attrNames = {};
	for (let i = 0; i < matches.length; i++) {
		if (matches[i][1].length === 0) return getErrorObject("InvalidAttr", "Attribute '" + matches[i][2] + "' has no space in starting.", getPositionFromMatch(matches[i]));
		else if (matches[i][3] !== void 0 && matches[i][4] === void 0) return getErrorObject("InvalidAttr", "Attribute '" + matches[i][2] + "' is without value.", getPositionFromMatch(matches[i]));
		else if (matches[i][3] === void 0 && !options.allowBooleanAttributes) return getErrorObject("InvalidAttr", "boolean attribute '" + matches[i][2] + "' is not allowed.", getPositionFromMatch(matches[i]));
		const attrName = matches[i][2];
		if (!validateAttrName(attrName)) return getErrorObject("InvalidAttr", "Attribute '" + attrName + "' is an invalid name.", getPositionFromMatch(matches[i]));
		if (!Object.prototype.hasOwnProperty.call(attrNames, attrName)) attrNames[attrName] = 1;
		else return getErrorObject("InvalidAttr", "Attribute '" + attrName + "' is repeated.", getPositionFromMatch(matches[i]));
	}
	return true;
}
function validateNumberAmpersand(xmlData, i) {
	let re = /\d/;
	if (xmlData[i] === "x") {
		i++;
		re = /[\da-fA-F]/;
	}
	for (; i < xmlData.length; i++) {
		if (xmlData[i] === ";") return i;
		if (!xmlData[i].match(re)) break;
	}
	return -1;
}
function validateAmpersand(xmlData, i) {
	i++;
	if (xmlData[i] === ";") return -1;
	if (xmlData[i] === "#") {
		i++;
		return validateNumberAmpersand(xmlData, i);
	}
	let count = 0;
	for (; i < xmlData.length; i++, count++) {
		if (xmlData[i].match(/\w/) && count < 20) continue;
		if (xmlData[i] === ";") break;
		return -1;
	}
	return i;
}
function getErrorObject(code, message, lineNumber) {
	return { err: {
		code,
		msg: message,
		line: lineNumber.line || lineNumber,
		col: lineNumber.col
	} };
}
function validateAttrName(attrName) {
	return isName(attrName);
}
function validateTagName(tagname) {
	return isName(tagname);
}
function getLineNumberForPosition(xmlData, index) {
	const lines = xmlData.substring(0, index).split(/\r?\n/);
	return {
		line: lines.length,
		col: lines[lines.length - 1].length + 1
	};
}
function getPositionFromMatch(match) {
	return match.startIndex + match[1].length;
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/@nodable+entities@3.1.0/node_modules/@nodable/entities/src/entities.js
/**
* Currency Symbols
* @type {Record<string, string>}
*/
var CURRENCY = {
	cent: "¢",
	pound: "£",
	curren: "¤",
	yen: "¥",
	euro: "€",
	dollar: "$",
	fnof: "ƒ",
	inr: "₹",
	af: "؋",
	birr: "ብር",
	peso: "₱",
	rub: "₽",
	won: "₩",
	yuan: "¥",
	cedil: "¸"
};
var XML = {
	amp: "&",
	apos: "'",
	gt: ">",
	lt: "<",
	quot: "\""
};
var COMMON_HTML = {
	nbsp: "\xA0",
	copy: "©",
	reg: "®",
	trade: "™",
	mdash: "—",
	ndash: "–",
	hellip: "…",
	laquo: "«",
	raquo: "»",
	lsquo: "‘",
	rsquo: "’",
	ldquo: "“",
	rdquo: "”",
	bull: "•",
	para: "¶",
	sect: "§",
	deg: "°",
	frac12: "½",
	frac14: "¼",
	frac34: "¾"
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/@nodable+entities@3.1.0/node_modules/@nodable/entities/src/EntityDecoder.js
/**
* Action constants for `onExternalEntity` and `onInputEntity` hooks.
*
* Use these instead of raw strings to avoid typos:
*
* @example
* import EntityDecoder, { ENTITY_ACTION } from './EntityDecoder.js';
* const dec = new EntityDecoder({
*   onInputEntity: (name, value) => ENTITY_ACTION.BLOCK,
* });
*/
var ENTITY_ACTION = Object.freeze({
	/** Resolve and expand the entity normally. */
	ALLOW: "allow",
	/** Silently skip this entity — it will not be registered. */
	BLOCK: "block",
	/** Throw an error, aborting entity registration entirely. */
	THROW: "throw"
});
var SPECIAL_CHARS = /* @__PURE__ */ new Set("!?\\\\/[]$%{}^&*()<>|+");
/**
* Validate that an entity name contains no dangerous characters.
* @param {string} name
* @returns {string} the name, unchanged
* @throws {Error} on invalid characters
*/
function validateEntityName$1(name) {
	if (name[0] === "#") throw new Error(`[EntityReplacer] Invalid character '#' in entity name: "${name}"`);
	for (const ch of name) if (SPECIAL_CHARS.has(ch)) throw new Error(`[EntityReplacer] Invalid character '${ch}' in entity name: "${name}"`);
	return name;
}
/**
* Merge one or more entity maps into a flat name→string map.
* Accepts either:
*   - plain string values:             { amp: '&' }
*   - legacy {regex,val} / {regx,val}: { lt: { regex: /.../, val: '<' } }
*
* Values containing '&' are skipped (recursive expansion risk).
*
* @param {...object} maps
* @returns {Record<string, string>}
*/
function mergeEntityMaps(...maps) {
	const out = Object.create(null);
	for (const map of maps) {
		if (!map) continue;
		for (const key of Object.keys(map)) {
			const raw = map[key];
			if (typeof raw === "string") out[key] = raw;
			else if (raw && typeof raw === "object" && raw.val !== void 0) {
				const val = raw.val;
				if (typeof val === "string") out[key] = val;
			}
		}
	}
	return out;
}
var LIMIT_TIER_EXTERNAL = "external";
var LIMIT_TIER_BASE = "base";
var LIMIT_TIER_ALL = "all";
/**
* Resolve `applyLimitsTo` option into a normalised Set of tier strings.
* Accepted values: 'external' | 'base' | 'all' | string[]
* Default: 'external' (only untrusted injected entities are counted).
* @param {string|string[]|undefined} raw
* @returns {Set<string>}
*/
function parseLimitTiers(raw) {
	if (!raw || raw === LIMIT_TIER_EXTERNAL) return new Set([LIMIT_TIER_EXTERNAL]);
	if (raw === LIMIT_TIER_ALL) return new Set([LIMIT_TIER_ALL]);
	if (raw === LIMIT_TIER_BASE) return new Set([LIMIT_TIER_BASE]);
	if (Array.isArray(raw)) return new Set(raw);
	return new Set([LIMIT_TIER_EXTERNAL]);
}
var NCR_LEVEL = Object.freeze({
	allow: 0,
	leave: 1,
	remove: 2,
	throw: 3
});
var XML10_ALLOWED_C0 = new Set([
	9,
	10,
	13
]);
/**
* Parse the `ncr` constructor option into flat, hot-path-friendly fields.
* @param {object|undefined} ncr
* @returns {{ xmlVersion: number, onLevel: number, nullLevel: number }}
*/
function parseNCRConfig(ncr) {
	if (!ncr) return {
		xmlVersion: 1,
		onLevel: NCR_LEVEL.allow,
		nullLevel: NCR_LEVEL.remove
	};
	const xmlVersion = ncr.xmlVersion === 1.1 ? 1.1 : 1;
	const onLevel = NCR_LEVEL[ncr.onNCR] ?? NCR_LEVEL.allow;
	const nullLevel = NCR_LEVEL[ncr.nullNCR] ?? NCR_LEVEL.remove;
	return {
		xmlVersion,
		onLevel,
		nullLevel: Math.max(nullLevel, NCR_LEVEL.remove)
	};
}
/**
* Single-pass, zero-regex entity replacer for XML/HTML content.
*
* Algorithm: scan the string once for '&', read to ';', resolve via map
* or direct codepoint conversion, build output chunks, join once at the end.
*
* Entity lookup priority (highest → lowest):
*   1. input / runtime  (DOCTYPE entities for current document)
*   2. persistent external (survive across documents)
*   3. base named map   (DEFAULT_XML_ENTITIES + user-supplied namedEntities)
*
* Both input and external resolve as the 'external' tier for limit purposes.
* Base map entities resolve as the 'base' tier.
*
* Numeric / hex references (&#NNN; / &#xHH;) are resolved directly via
* String.fromCodePoint() — no map needed. They count as 'base' tier.
*
* @example
* const replacer = new EntityReplacer({ namedEntities: COMMON_HTML });
* replacer.setExternalEntities({ brand: 'Acme' });
*
* const instance = replacer.reset();
* instance.addInputEntities({ version: '1.0' });
* instance.encode('&brand; v&version; &lt;'); // 'Acme v1.0 <'
*/
var EntityDecoder = class {
	/**
	* @param {object} [options]
	* @param {object|null}  [options.namedEntities]        — extra named entities merged into base map
	* @param {object}  [options.limit]                 — security limits
	* @param {number}       [options.limit.maxTotalExpansions=0]  — 0 = unlimited
	* @param {number}       [options.limit.maxExpandedLength=0]   — 0 = unlimited
	* @param {'external'|'base'|'all'|string[]} [options.limit.applyLimitsTo='external']
	*   Which entity tiers count against the security limits:
	*   - 'external' (default) — only input/runtime + persistent external entities
	*   - 'base'               — only DEFAULT_XML_ENTITIES + namedEntities
	*   - 'all'                — every entity regardless of tier
	*   - string[]             — explicit combination, e.g. ['external', 'base']
	* @param {((resolved: string, original: string) => string)|null} [options.postCheck=null]
	* @param {string[]} [options.remove=[]] — entity names (e.g. ['nbsp', '#13']) to delete (replace with empty string)
	* @param {string[]} [options.leave=[]]  — entity names to keep as literal (unchanged in output)
	* @param {object}   [options.ncr]       — Numeric Character Reference controls
	* @param {1.0|1.1}  [options.ncr.xmlVersion=1.0]
	*   XML version governing which codepoint ranges are restricted:
	*   - 1.0 — C0 controls U+0001–U+001F (except U+0009/000A/000D) are prohibited
	*   - 1.1 — C0 controls are allowed when written as NCRs; C1 (U+007F–U+009F) decoded as-is
	* @param {'allow'|'leave'|'remove'|'throw'} [options.ncr.onNCR='allow']
	*   Base action for numeric references. Severity order: allow < leave < remove < throw.
	*   For codepoint ranges that carry a minimum level (surrogates → remove, XML 1.0 C0 → remove),
	*   the effective action is max(onNCR, rangeMinimum).
	* @param {'remove'|'throw'} [options.ncr.nullNCR='remove']
	*   Action for U+0000 (null). 'allow' and 'leave' are clamped to 'remove' since null is never safe.
	* @param {((name: string, value: string) => 'allow'|'block'|'throw')|null} [options.onExternalEntity=null]
	*   Hook called when an external entity is registered via `setExternalEntities()` or
	*   `addExternalEntity()`. Return `ENTITY_ACTION.ALLOW` to accept the entity,
	*   `ENTITY_ACTION.BLOCK` to silently skip it, or `ENTITY_ACTION.THROW` to abort with an error.
	* @param {((name: string, value: string) => 'allow'|'block'|'throw')|null} [options.onInputEntity=null]
	*   Hook called when an input entity is registered via `addInputEntities()`. Return
	*   `ENTITY_ACTION.ALLOW` to accept, `ENTITY_ACTION.BLOCK` to silently skip, or
	*   `ENTITY_ACTION.THROW` to abort with an error.
	*/
	constructor(options = {}) {
		this._limit = options.limit || {};
		this._maxTotalExpansions = this._limit.maxTotalExpansions || 0;
		this._maxExpandedLength = this._limit.maxExpandedLength || 0;
		this._postCheck = typeof options.postCheck === "function" ? options.postCheck : (r) => r;
		this._limitTiers = parseLimitTiers(this._limit.applyLimitsTo ?? LIMIT_TIER_EXTERNAL);
		this._numericAllowed = options.numericAllowed ?? true;
		this._baseMap = mergeEntityMaps(XML, options.namedEntities || null);
		/** @type {Record<string, string>} */
		this._externalMap = Object.create(null);
		/** @type {Record<string, string>} */
		this._inputMap = Object.create(null);
		this._totalExpansions = 0;
		this._expandedLength = 0;
		/** @type {Set<string>} */
		this._removeSet = new Set(options.remove && Array.isArray(options.remove) ? options.remove : []);
		/** @type {Set<string>} */
		this._leaveSet = new Set(options.leave && Array.isArray(options.leave) ? options.leave : []);
		const ncrCfg = parseNCRConfig(options.ncr);
		this._ncrXmlVersion = ncrCfg.xmlVersion;
		this._ncrOnLevel = ncrCfg.onLevel;
		this._ncrNullLevel = ncrCfg.nullLevel;
		/** @type {((name: string, value: string) => 'allow'|'block'|'throw')|null} */
		this._onExternalEntity = typeof options.onExternalEntity === "function" ? options.onExternalEntity : null;
		/** @type {((name: string, value: string) => 'allow'|'block'|'throw')|null} */
		this._onInputEntity = typeof options.onInputEntity === "function" ? options.onInputEntity : null;
	}
	/**
	* Invoke a registration hook for a single entity name/value pair.
	* Returns true when the entity should be accepted, false when it should be
	* silently skipped (BLOCK), and throws when the hook returns THROW.
	*
	* @param {((name: string, value: string) => 'allow'|'block'|'throw')|null} hook
	* @param {string} name
	* @param {string} value
	* @param {string} context  — used in error messages ('external' | 'input')
	* @returns {boolean}  true = accept, false = skip
	*/
	_applyRegistrationHook(hook, name, value, context) {
		if (!hook) return true;
		const action = hook(name, value);
		if (action === ENTITY_ACTION.BLOCK) return false;
		if (action === ENTITY_ACTION.THROW) throw new Error(`[EntityDecoder] Registration of ${context} entity "&${name};" was rejected by hook`);
		return true;
	}
	/**
	* Replace the full set of persistent external entities.
	* All keys are validated — throws on invalid characters.
	* If `onExternalEntity` is set, it is called once per entry; entries that
	* return `ENTITY_ACTION.BLOCK` are silently omitted, `ENTITY_ACTION.THROW`
	* aborts the whole call.
	* @param {Record<string, string | { regex?: RegExp, val: string }>} map
	*/
	setExternalEntities(map) {
		if (map) for (const key of Object.keys(map)) validateEntityName$1(key);
		if (!this._onExternalEntity) {
			this._externalMap = mergeEntityMaps(map);
			return;
		}
		const flat = mergeEntityMaps(map);
		const filtered = Object.create(null);
		for (const [name, value] of Object.entries(flat)) if (this._applyRegistrationHook(this._onExternalEntity, name, value, "external")) filtered[name] = value;
		this._externalMap = filtered;
	}
	/**
	* Add a single persistent external entity.
	* If `onExternalEntity` is set it is called before the entity is stored;
	* `ENTITY_ACTION.BLOCK` silently skips storage, `ENTITY_ACTION.THROW` raises.
	* @param {string} key
	* @param {string} value
	*/
	addExternalEntity(key, value) {
		validateEntityName$1(key);
		if (typeof value === "string" && value.indexOf("&") === -1) {
			if (this._applyRegistrationHook(this._onExternalEntity, key, value, "external")) this._externalMap[key] = value;
		}
	}
	/**
	* Inject DOCTYPE entities for the current document.
	* Also resets per-document expansion counters.
	* If `onInputEntity` is set it is called once per entry; entries returning
	* `ENTITY_ACTION.BLOCK` are silently omitted, `ENTITY_ACTION.THROW` aborts.
	* @param {Record<string, string | { regx?: RegExp, regex?: RegExp, val: string }>} map
	*/
	addInputEntities(map) {
		this._totalExpansions = 0;
		this._expandedLength = 0;
		if (!this._onInputEntity) {
			this._inputMap = mergeEntityMaps(map);
			return;
		}
		const flat = mergeEntityMaps(map);
		const filtered = Object.create(null);
		for (const [name, value] of Object.entries(flat)) if (this._applyRegistrationHook(this._onInputEntity, name, value, "input")) filtered[name] = value;
		this._inputMap = filtered;
	}
	/**
	* Wipe input/runtime entities and reset counters.
	* Call this before processing each new document.
	* @returns {this}
	*/
	reset() {
		this._inputMap = Object.create(null);
		this._totalExpansions = 0;
		this._expandedLength = 0;
		return this;
	}
	/**
	* Update the XML version used for NCR classification.
	* Call this as soon as the document's `<?xml version="...">` declaration is parsed.
	* @param {1.0|1.1|number} version
	*/
	setXmlVersion(version) {
		this._ncrXmlVersion = version === 1.1 ? 1.1 : 1;
	}
	/**
	* Replace all entity references in `str` in a single pass.
	*
	* @param {string} str
	* @returns {string}
	*/
	decode(str) {
		if (typeof str !== "string" || str.length === 0) return str;
		if (str.indexOf("&") === -1) return str;
		const original = str;
		const chunks = [];
		const len = str.length;
		let last = 0;
		let i = 0;
		const limitExpansions = this._maxTotalExpansions > 0;
		const limitLength = this._maxExpandedLength > 0;
		const checkLimits = limitExpansions || limitLength;
		while (i < len) {
			if (str.charCodeAt(i) !== 38) {
				i++;
				continue;
			}
			let j = i + 1;
			while (j < len && str.charCodeAt(j) !== 59 && j - i <= 32) j++;
			if (j >= len || str.charCodeAt(j) !== 59) {
				i++;
				continue;
			}
			const token = str.slice(i + 1, j);
			if (token.length === 0) {
				i++;
				continue;
			}
			let replacement;
			let tier;
			if (this._removeSet.has(token)) {
				replacement = "";
				if (tier === void 0) tier = LIMIT_TIER_EXTERNAL;
			} else if (this._leaveSet.has(token)) {
				i++;
				continue;
			} else if (token.charCodeAt(0) === 35) {
				const ncrResult = this._resolveNCR(token);
				if (ncrResult === void 0) {
					i++;
					continue;
				}
				replacement = ncrResult;
				tier = LIMIT_TIER_BASE;
			} else {
				const resolved = this._resolveName(token);
				replacement = resolved?.value;
				tier = resolved?.tier;
			}
			if (replacement === void 0) {
				i++;
				continue;
			}
			if (i > last) chunks.push(str.slice(last, i));
			chunks.push(replacement);
			last = j + 1;
			i = last;
			if (checkLimits && this._tierCounts(tier)) {
				if (limitExpansions) {
					this._totalExpansions++;
					if (this._totalExpansions > this._maxTotalExpansions) throw new Error(`[EntityReplacer] Entity expansion count limit exceeded: ${this._totalExpansions} > ${this._maxTotalExpansions}`);
				}
				if (limitLength) {
					const delta = replacement.length - (token.length + 2);
					if (delta > 0) {
						this._expandedLength += delta;
						if (this._expandedLength > this._maxExpandedLength) throw new Error(`[EntityReplacer] Expanded content length limit exceeded: ${this._expandedLength} > ${this._maxExpandedLength}`);
					}
				}
			}
		}
		if (last < len) chunks.push(str.slice(last));
		const result = chunks.length === 0 ? str : chunks.join("");
		return this._postCheck(result, original);
	}
	/**
	* Returns true if a resolved entity of the given tier should count
	* against the expansion/length limits.
	* @param {string} tier  — LIMIT_TIER_EXTERNAL | LIMIT_TIER_BASE
	* @returns {boolean}
	*/
	_tierCounts(tier) {
		if (this._limitTiers.has(LIMIT_TIER_ALL)) return true;
		return this._limitTiers.has(tier);
	}
	/**
	* Resolve a named entity token (without & and ;).
	* Priority: inputMap > externalMap > baseMap
	* Returns the resolved value tagged with its limit tier.
	*
	* @param {string} name
	* @returns {{ value: string, tier: string }|undefined}
	*/
	_resolveName(name) {
		if (name in this._inputMap) return {
			value: this._inputMap[name],
			tier: LIMIT_TIER_EXTERNAL
		};
		if (name in this._externalMap) return {
			value: this._externalMap[name],
			tier: LIMIT_TIER_EXTERNAL
		};
		if (name in this._baseMap) return {
			value: this._baseMap[name],
			tier: LIMIT_TIER_BASE
		};
	}
	/**
	* Classify a codepoint and return the minimum action level that must be applied.
	* Returns -1 when no minimum is imposed (normal allow path).
	*
	* Ranges checked (in priority order):
	*   1. U+0000            — null, governed by nullNCR (always ≥ remove)
	*   2. U+D800–U+DFFF     — surrogates, always prohibited (min: remove)
	*   3. U+0001–U+001F \ {0x09,0x0A,0x0D}  — XML 1.0 restricted C0 (min: remove)
	*      (skipped in XML 1.1 — C0 controls are allowed when written as NCRs)
	*
	* @param {number} cp  — codepoint
	* @returns {number}   — minimum NCR_LEVEL value, or -1 for no restriction
	*/
	_classifyNCR(cp) {
		if (cp === 0) return this._ncrNullLevel;
		if (cp >= 55296 && cp <= 57343) return NCR_LEVEL.remove;
		if (this._ncrXmlVersion === 1) {
			if (cp >= 1 && cp <= 31 && !XML10_ALLOWED_C0.has(cp)) return NCR_LEVEL.remove;
		}
		return -1;
	}
	/**
	* Execute a resolved NCR action.
	*
	* @param {number} action   — NCR_LEVEL value
	* @param {string} token    — raw token (e.g. '#38') for error messages
	* @param {number} cp       — codepoint, used only for error messages
	* @returns {string|undefined}
	*   - decoded character string  → 'allow'
	*   - ''                        → 'remove'
	*   - undefined                 → 'leave' (caller must skip past '&' only)
	*   - throws Error              → 'throw'
	*/
	_applyNCRAction(action, token, cp) {
		switch (action) {
			case NCR_LEVEL.allow: return String.fromCodePoint(cp);
			case NCR_LEVEL.remove: return "";
			case NCR_LEVEL.leave: return;
			case NCR_LEVEL.throw: throw new Error(`[EntityDecoder] Prohibited numeric character reference &${token}; (U+${cp.toString(16).toUpperCase().padStart(4, "0")})`);
			default: return String.fromCodePoint(cp);
		}
	}
	/**
	* Full NCR resolution pipeline for a numeric token.
	*
	* Steps:
	*   1. Parse the codepoint (decimal or hex).
	*   2. Validate the raw codepoint range (NaN, <0, >0x10FFFF).
	*   3. If numericAllowed is false and no minimum restriction applies → leave as-is.
	*   4. Classify the codepoint to find the minimum required action level.
	*   5. Resolve effective action = max(onNCR, minimum).
	*   6. Apply and return.
	*
	* @param {string} token  — e.g. '#38', '#x26', '#X26'
	* @returns {string|undefined}
	*   - string (incl. '')  — replacement ('' = remove)
	*   - undefined          — leave original &token; as-is
	*/
	_resolveNCR(token) {
		const second = token.charCodeAt(1);
		let cp;
		if (second === 120 || second === 88) cp = parseInt(token.slice(2), 16);
		else cp = parseInt(token.slice(1), 10);
		if (Number.isNaN(cp) || cp < 0 || cp > 1114111) return void 0;
		const minimum = this._classifyNCR(cp);
		if (!this._numericAllowed && minimum < NCR_LEVEL.remove) return void 0;
		const effective = minimum === -1 ? this._ncrOnLevel : Math.max(this._ncrOnLevel, minimum);
		return this._applyNCRAction(effective, token, cp);
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/OptionsBuilder.js
var defaultOnDangerousProperty = (name) => {
	if (DANGEROUS_PROPERTY_NAMES.includes(name)) return "__" + name;
	return name;
};
var defaultOptions = {
	preserveOrder: false,
	attributeNamePrefix: "@_",
	attributesGroupName: false,
	textNodeName: "#text",
	ignoreAttributes: true,
	removeNSPrefix: false,
	allowBooleanAttributes: false,
	parseTagValue: true,
	parseAttributeValue: false,
	trimValues: true,
	cdataPropName: false,
	numberParseOptions: {
		hex: true,
		leadingZeros: true,
		eNotation: true,
		unicode: false
	},
	tagValueProcessor: function(tagName, val) {
		return val;
	},
	attributeValueProcessor: function(attrName, val) {
		return val;
	},
	stopNodes: [],
	alwaysCreateTextNode: false,
	isArray: () => false,
	commentPropName: false,
	unpairedTags: [],
	processEntities: true,
	htmlEntities: false,
	entityDecoder: null,
	ignoreDeclaration: false,
	ignorePiTags: false,
	transformTagName: false,
	transformAttributeName: false,
	updateTag: function(tagName, jPath, attrs) {
		return tagName;
	},
	captureMetaData: false,
	maxNestedTags: 100,
	strictReservedNames: true,
	jPath: true,
	onDangerousProperty: defaultOnDangerousProperty
};
/**
* Validates that a property name is safe to use
* @param {string} propertyName - The property name to validate
* @param {string} optionName - The option field name (for error message)
* @throws {Error} If property name is dangerous
*/
function validatePropertyName(propertyName, optionName) {
	if (typeof propertyName !== "string") return;
	const normalized = propertyName.toLowerCase();
	if (DANGEROUS_PROPERTY_NAMES.some((dangerous) => normalized === dangerous.toLowerCase())) throw new Error(`[SECURITY] Invalid ${optionName}: "${propertyName}" is a reserved JavaScript keyword that could cause prototype pollution`);
	if (criticalProperties.some((dangerous) => normalized === dangerous.toLowerCase())) throw new Error(`[SECURITY] Invalid ${optionName}: "${propertyName}" is a reserved JavaScript keyword that could cause prototype pollution`);
}
/**
* Normalizes processEntities option for backward compatibility
* @param {boolean|object} value 
* @returns {object} Always returns normalized object
*/
function normalizeProcessEntities(value, htmlEntities) {
	if (typeof value === "boolean") return {
		enabled: value,
		maxEntitySize: 1e4,
		maxExpansionDepth: 1e4,
		maxTotalExpansions: Infinity,
		maxExpandedLength: 1e5,
		maxEntityCount: 1e3,
		allowedTags: null,
		tagFilter: null,
		appliesTo: "all"
	};
	if (typeof value === "object" && value !== null) return {
		enabled: value.enabled !== false,
		maxEntitySize: Math.max(1, value.maxEntitySize ?? 1e4),
		maxExpansionDepth: Math.max(1, value.maxExpansionDepth ?? 1e4),
		maxTotalExpansions: Math.max(1, value.maxTotalExpansions ?? Infinity),
		maxExpandedLength: Math.max(1, value.maxExpandedLength ?? 1e5),
		maxEntityCount: Math.max(1, value.maxEntityCount ?? 1e3),
		allowedTags: value.allowedTags ?? null,
		tagFilter: value.tagFilter ?? null,
		appliesTo: value.appliesTo ?? "all"
	};
	return normalizeProcessEntities(true);
}
var buildOptions = function(options) {
	const built = Object.assign({}, defaultOptions, options);
	const propertyNameOptions = [
		{
			value: built.attributeNamePrefix,
			name: "attributeNamePrefix"
		},
		{
			value: built.attributesGroupName,
			name: "attributesGroupName"
		},
		{
			value: built.textNodeName,
			name: "textNodeName"
		},
		{
			value: built.cdataPropName,
			name: "cdataPropName"
		},
		{
			value: built.commentPropName,
			name: "commentPropName"
		}
	];
	for (const { value, name } of propertyNameOptions) if (value) validatePropertyName(value, name);
	if (built.onDangerousProperty === null) built.onDangerousProperty = defaultOnDangerousProperty;
	built.processEntities = normalizeProcessEntities(built.processEntities, built.htmlEntities);
	built.unpairedTagsSet = new Set(built.unpairedTags);
	if (built.stopNodes && Array.isArray(built.stopNodes)) built.stopNodes = built.stopNodes.map((node) => {
		if (typeof node === "string" && node.startsWith("*.")) return ".." + node.substring(2);
		return node;
	});
	return built;
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/xmlNode.js
var METADATA_SYMBOL$1;
if (typeof Symbol !== "function") METADATA_SYMBOL$1 = "@@xmlMetadata";
else METADATA_SYMBOL$1 = Symbol("XML Node Metadata");
var XmlNode = class {
	constructor(tagname) {
		this.tagname = tagname;
		this.child = [];
		this[":@"] = Object.create(null);
	}
	add(key, val) {
		if (key === "__proto__") key = "#__proto__";
		this.child.push({ [key]: val });
	}
	addChild(node, startIndex) {
		if (node.tagname === "__proto__") node.tagname = "#__proto__";
		if (node[":@"] && Object.keys(node[":@"]).length > 0) this.child.push({
			[node.tagname]: node.child,
			[":@"]: node[":@"]
		});
		else this.child.push({ [node.tagname]: node.child });
		this.addStartIndex(startIndex);
	}
	addStartIndex(startIndex) {
		if (startIndex !== void 0) this.child[this.child.length - 1][METADATA_SYMBOL$1] = { startIndex };
	}
	addEndIndex(endIndex) {
		const lastChild = this.child[this.child.length - 1];
		if (lastChild !== void 0 && lastChild[METADATA_SYMBOL$1] !== void 0 && lastChild[METADATA_SYMBOL$1].endIndex === void 0) lastChild[METADATA_SYMBOL$1].endIndex = endIndex;
	}
	/** symbol used for metadata */
	static getMetaDataSymbol() {
		return METADATA_SYMBOL$1;
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/xml-naming@0.3.0/node_modules/xml-naming/src/index.js
/**
* xml-naming
* Validates XML Name productions as defined in the XML 1.0 and 1.1 specifications.
* Covers: Name, NCName, QName, NMToken, NMTokens
*
* XML 1.0 spec: https://www.w3.org/TR/xml/#NT-Name
* XML 1.1 spec: https://www.w3.org/TR/xml11/#NT-NameStartChar
* XML NS spec:  https://www.w3.org/TR/xml-names/#NT-NCName
*/
var nameStartChar10 = ":A-Za-z_À-ÖØ-öø-˿Ͱ-ͽͿ-҆҈-῿‌-‍⁰-↏Ⰰ-⿯、-퟿豈-﷏ﷰ-�";
var nameChar10 = nameStartChar10 + "\\-\\.\\d·̀-ͯ‿-⁀";
var nameStartChar11 = ":A-Za-z_À-˿Ͱ-ͽͿ-҆҈-῿‌-‍⁰-↏Ⰰ-⿯、-퟿豈-﷏ﷰ-�𐀀-󯿿";
var nameChar11 = nameStartChar11 + "\\-\\.\\d·̀-ͯ҇‿-⁀";
var buildRegexes = (startChar, char, flags = "") => {
	const ncNamePat = `[${startChar.replace(":", "")}][${char.replace(":", "")}]*`;
	return {
		name: new RegExp(`^[${startChar}][${char}]*$`, flags),
		ncName: new RegExp(`^${ncNamePat}$`, flags),
		qName: new RegExp(`^${ncNamePat}(?::${ncNamePat})?$`, flags),
		nmToken: new RegExp(`^[${char}]+$`, flags),
		nmTokens: new RegExp(`^[${char}]+(?:\\s+[${char}]+)*$`, flags)
	};
};
var regexes10 = buildRegexes(nameStartChar10, nameChar10);
var regexes11 = buildRegexes(nameStartChar11, nameChar11, "u");
var nameStartCharAscii = ":A-Za-z_";
var regexesAscii = buildRegexes(nameStartCharAscii, nameStartCharAscii + "\\-\\.\\d");
var getRegexes = (xmlVersion = "1.0", asciiOnly = false) => {
	if (asciiOnly) return regexesAscii;
	return xmlVersion === "1.1" ? regexes11 : regexes10;
};
/**
* Returns true if the string is a valid QName (Qualified Name).
* Allows exactly one colon as a prefix separator: prefix:localName.
* Used for: element and attribute names in namespace-aware XML/SVG.
*
* @param {{ xmlVersion?: '1.0'|'1.1', asciiOnly?: boolean }} [opts]
*   asciiOnly: skip unicode-aware matching, ASCII names only (default false).
*/
var qName = (str, { xmlVersion = "1.0", asciiOnly = false } = {}) => getRegexes(xmlVersion, asciiOnly).qName.test(str);
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/DocTypeReader.js
var DocTypeReader = class {
	constructor(options, xmlVersion) {
		this.suppressValidationErr = !options;
		this.options = options;
		this.xmlVersion = xmlVersion || 1;
	}
	setXmlVersion(xmlVersion = 1) {
		this.xmlVersion = xmlVersion;
	}
	readDocType(xmlData, i) {
		const entities = Object.create(null);
		let entityCount = 0;
		if (xmlData[i + 3] === "O" && xmlData[i + 4] === "C" && xmlData[i + 5] === "T" && xmlData[i + 6] === "Y" && xmlData[i + 7] === "P" && xmlData[i + 8] === "E") {
			i = i + 9;
			let angleBracketsCount = 1;
			let hasBody = false, comment = false;
			let quoteChar = null;
			let exp = "";
			for (; i < xmlData.length; i++) {
				if (quoteChar !== null) {
					if (xmlData[i] === quoteChar) quoteChar = null;
					exp += xmlData[i];
					continue;
				}
				if (!hasBody && !comment && (xmlData[i] === "\"" || xmlData[i] === "'")) {
					quoteChar = xmlData[i];
					exp += xmlData[i];
					continue;
				}
				if (xmlData[i] === "<" && !comment) {
					if (hasBody && hasSeq(xmlData, "!ENTITY", i)) {
						i += 7;
						let entityName, val;
						[entityName, val, i] = this.readEntityExp(xmlData, i + 1, this.suppressValidationErr);
						if (val.indexOf("&") === -1) {
							if (this.options.enabled !== false && this.options.maxEntityCount != null && entityCount >= this.options.maxEntityCount) throw new Error(`Entity count (${entityCount + 1}) exceeds maximum allowed (${this.options.maxEntityCount})`);
							entities[entityName] = val;
							entityCount++;
						}
					} else if (hasBody && hasSeq(xmlData, "!ELEMENT", i)) {
						i += 8;
						const { index } = this.readElementExp(xmlData, i + 1);
						i = index;
					} else if (hasBody && hasSeq(xmlData, "!ATTLIST", i)) i += 8;
					else if (hasBody && hasSeq(xmlData, "!NOTATION", i)) {
						i += 9;
						const { index } = this.readNotationExp(xmlData, i + 1, this.suppressValidationErr);
						i = index;
					} else if (hasSeq(xmlData, "!--", i)) comment = true;
					else throw new Error(`Invalid DOCTYPE`);
					angleBracketsCount++;
					exp = "";
				} else if (xmlData[i] === ">") {
					if (comment) {
						if (xmlData[i - 1] === "-" && xmlData[i - 2] === "-") {
							comment = false;
							angleBracketsCount--;
						}
					} else angleBracketsCount--;
					if (angleBracketsCount === 0) break;
				} else if (xmlData[i] === "[") hasBody = true;
				else exp += xmlData[i];
			}
			if (quoteChar !== null || angleBracketsCount !== 0) throw new Error(`Unclosed DOCTYPE`);
		} else throw new Error(`Invalid Tag instead of DOCTYPE`);
		return {
			entities,
			i
		};
	}
	readEntityExp(xmlData, i) {
		i = skipWhitespace(xmlData, i);
		const startIndex = i;
		while (i < xmlData.length && !/\s/.test(xmlData[i]) && xmlData[i] !== "\"" && xmlData[i] !== "'") i++;
		let entityName = xmlData.substring(startIndex, i);
		validateEntityName(entityName, { xmlVersion: this.xmlVersion });
		i = skipWhitespace(xmlData, i);
		if (!this.suppressValidationErr) {
			if (xmlData.substring(i, i + 6).toUpperCase() === "SYSTEM") throw new Error("External entities are not supported");
			else if (xmlData[i] === "%") throw new Error("Parameter entities are not supported");
		}
		let entityValue = "";
		[i, entityValue] = this.readIdentifierVal(xmlData, i, "entity");
		if (this.options.enabled !== false && this.options.maxEntitySize != null && entityValue.length > this.options.maxEntitySize) throw new Error(`Entity "${entityName}" size (${entityValue.length}) exceeds maximum allowed size (${this.options.maxEntitySize})`);
		i--;
		return [
			entityName,
			entityValue,
			i
		];
	}
	readNotationExp(xmlData, i) {
		i = skipWhitespace(xmlData, i);
		const startIndex = i;
		while (i < xmlData.length && !/\s/.test(xmlData[i])) i++;
		let notationName = xmlData.substring(startIndex, i);
		!this.suppressValidationErr && validateEntityName(notationName, { xmlVersion: this.xmlVersion });
		i = skipWhitespace(xmlData, i);
		const identifierType = xmlData.substring(i, i + 6).toUpperCase();
		if (!this.suppressValidationErr && identifierType !== "SYSTEM" && identifierType !== "PUBLIC") throw new Error(`Expected SYSTEM or PUBLIC, found "${identifierType}"`);
		i += identifierType.length;
		i = skipWhitespace(xmlData, i);
		let publicIdentifier = null;
		let systemIdentifier = null;
		if (identifierType === "PUBLIC") {
			[i, publicIdentifier] = this.readIdentifierVal(xmlData, i, "publicIdentifier");
			i = skipWhitespace(xmlData, i);
			if (xmlData[i] === "\"" || xmlData[i] === "'") [i, systemIdentifier] = this.readIdentifierVal(xmlData, i, "systemIdentifier");
		} else if (identifierType === "SYSTEM") {
			[i, systemIdentifier] = this.readIdentifierVal(xmlData, i, "systemIdentifier");
			if (!this.suppressValidationErr && !systemIdentifier) throw new Error("Missing mandatory system identifier for SYSTEM notation");
		}
		return {
			notationName,
			publicIdentifier,
			systemIdentifier,
			index: --i
		};
	}
	readIdentifierVal(xmlData, i, type) {
		let identifierVal = "";
		const startChar = xmlData[i];
		if (startChar !== "\"" && startChar !== "'") throw new Error(`Expected quoted string, found "${startChar}"`);
		i++;
		const startIndex = i;
		while (i < xmlData.length && xmlData[i] !== startChar) i++;
		identifierVal = xmlData.substring(startIndex, i);
		if (xmlData[i] !== startChar) throw new Error(`Unterminated ${type} value`);
		i++;
		return [i, identifierVal];
	}
	readElementExp(xmlData, i) {
		i = skipWhitespace(xmlData, i);
		const startIndex = i;
		while (i < xmlData.length && !/\s/.test(xmlData[i])) i++;
		let elementName = xmlData.substring(startIndex, i);
		if (!this.suppressValidationErr && !qName(elementName, { xmlVersion: this.xmlVersion })) throw new Error(`Invalid element name: "${elementName}"`);
		i = skipWhitespace(xmlData, i);
		let contentModel = "";
		if (xmlData[i] === "E" && hasSeq(xmlData, "MPTY", i)) i += 4;
		else if (xmlData[i] === "A" && hasSeq(xmlData, "NY", i)) i += 2;
		else if (xmlData[i] === "(") {
			i++;
			const startIndex = i;
			while (i < xmlData.length && xmlData[i] !== ")") i++;
			contentModel = xmlData.substring(startIndex, i);
			if (xmlData[i] !== ")") throw new Error("Unterminated content model");
		} else if (!this.suppressValidationErr) throw new Error(`Invalid Element Expression, found "${xmlData[i]}"`);
		return {
			elementName,
			contentModel: contentModel.trim(),
			index: i
		};
	}
	readAttlistExp(xmlData, i) {
		i = skipWhitespace(xmlData, i);
		let startIndex = i;
		while (i < xmlData.length && !/\s/.test(xmlData[i])) i++;
		let elementName = xmlData.substring(startIndex, i);
		validateEntityName(elementName, { xmlVersion: this.xmlVersion });
		i = skipWhitespace(xmlData, i);
		startIndex = i;
		while (i < xmlData.length && !/\s/.test(xmlData[i])) i++;
		let attributeName = xmlData.substring(startIndex, i);
		if (!validateEntityName(attributeName, { xmlVersion: this.xmlVersion })) throw new Error(`Invalid attribute name: "${attributeName}"`);
		i = skipWhitespace(xmlData, i);
		let attributeType = "";
		if (xmlData.substring(i, i + 8).toUpperCase() === "NOTATION") {
			attributeType = "NOTATION";
			i += 8;
			i = skipWhitespace(xmlData, i);
			if (xmlData[i] !== "(") throw new Error(`Expected '(', found "${xmlData[i]}"`);
			i++;
			let allowedNotations = [];
			while (i < xmlData.length && xmlData[i] !== ")") {
				const startIndex = i;
				while (i < xmlData.length && xmlData[i] !== "|" && xmlData[i] !== ")") i++;
				let notation = xmlData.substring(startIndex, i);
				notation = notation.trim();
				if (!validateEntityName(notation, { xmlVersion: this.xmlVersion })) throw new Error(`Invalid notation name: "${notation}"`);
				allowedNotations.push(notation);
				if (xmlData[i] === "|") {
					i++;
					i = skipWhitespace(xmlData, i);
				}
			}
			if (xmlData[i] !== ")") throw new Error("Unterminated list of notations");
			i++;
			attributeType += " (" + allowedNotations.join("|") + ")";
		} else {
			const startIndex = i;
			while (i < xmlData.length && !/\s/.test(xmlData[i])) i++;
			attributeType += xmlData.substring(startIndex, i);
			if (!this.suppressValidationErr && ![
				"CDATA",
				"ID",
				"IDREF",
				"IDREFS",
				"ENTITY",
				"ENTITIES",
				"NMTOKEN",
				"NMTOKENS"
			].includes(attributeType.toUpperCase())) throw new Error(`Invalid attribute type: "${attributeType}"`);
		}
		i = skipWhitespace(xmlData, i);
		let defaultValue = "";
		if (xmlData.substring(i, i + 8).toUpperCase() === "#REQUIRED") {
			defaultValue = "#REQUIRED";
			i += 8;
		} else if (xmlData.substring(i, i + 7).toUpperCase() === "#IMPLIED") {
			defaultValue = "#IMPLIED";
			i += 7;
		} else [i, defaultValue] = this.readIdentifierVal(xmlData, i, "ATTLIST");
		return {
			elementName,
			attributeName,
			attributeType,
			defaultValue,
			index: i
		};
	}
};
var skipWhitespace = (data, index) => {
	while (index < data.length && /\s/.test(data[index])) index++;
	return index;
};
function hasSeq(data, seq, i) {
	for (let j = 0; j < seq.length; j++) if (seq[j] !== data[i + j + 1]) return false;
	return true;
}
function validateEntityName(name, xmlVersion) {
	if (qName(name, { xmlVersion })) return name;
	else throw new Error(`Invalid entity name ${name}`);
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/anynum@1.0.1/node_modules/anynum/digitTable.js
/**
* Flat lookup table: maps Unicode code point → ASCII digit (0-9).
* Only decimal digit characters (Unicode category Nd) are included.
*
* Strategy: Int32Array of size (maxCodePoint - minCodePoint + 1).
* Value 0xFF means "not a digit". Value 0-9 is the ASCII digit value.
* This gives O(1) lookup with no branching, no bisect, no loop.
*
* Memory: range is 0x0660 to 0x1FBF0 → ~129,936 entries × 1 byte = ~127 KB.
* Acceptable for a one-time init; lookup is a single array index.
*/
var SCRIPT_ZEROS = [
	48,
	1632,
	1776,
	2406,
	2534,
	2662,
	2790,
	2918,
	3046,
	3174,
	3302,
	3430,
	3558,
	3664,
	3792,
	3872,
	4160,
	4240,
	6112,
	6160,
	6470,
	6608,
	6784,
	6800,
	6992,
	7088,
	7232,
	7248,
	65296,
	120782,
	120792,
	120802,
	120812,
	120822,
	66720,
	68912,
	69734,
	69872,
	69942,
	70096,
	70384,
	70736,
	70864,
	71248,
	71360,
	71472,
	71904,
	72016,
	72688,
	72784,
	73040,
	73120,
	73552,
	92768,
	92864,
	93008,
	123200,
	123632,
	124144,
	125264,
	130032
];
var HIGH_MAP = /* @__PURE__ */ new Map();
var LOW_MAX = 65535;
var LOW_MIN = 1632;
var TABLE_OFFSET = LOW_MIN;
var TABLE_SIZE = LOW_MAX - LOW_MIN + 1;
var TABLE = new Uint8Array(TABLE_SIZE).fill(255);
for (const zero of SCRIPT_ZEROS) for (let d = 0; d < 10; d++) {
	const cp = zero + d;
	if (cp <= LOW_MAX) TABLE[cp - TABLE_OFFSET] = d;
	else HIGH_MAP.set(cp, d);
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/anynum@1.0.1/node_modules/anynum/anynum.js
var CHAR_0 = 48;
var CHAR_9 = 57;
var CHAR_MINUS = 45;
var MINUS_SET = new Set([
	8722,
	65293,
	65123
]);
/**
* Normalize all Unicode decimal digit characters in a string to ASCII (0-9),
* and normalize Unicode minus variants to ASCII '-' (U+002D).
*
* Non-digit, non-minus characters are passed through unchanged.
*
* Performance design:
* - Fast path: if the string has no convertible characters, return it unchanged
*   (zero allocation).
* - BMP digits (0x0660..0xFFFF excl. surrogates): flat Uint8Array lookup (O(1)).
* - Supplementary plane digits (> 0xFFFF, encoded as surrogate pairs): Map lookup.
* - Minus variants: checked inline with a small fixed Set.
*
* @param {string} str
* @returns {string}
*/
function anynum(str) {
	if (typeof str !== "string") return str;
	const len = str.length;
	if (len === 0) return str;
	let firstHit = -1;
	for (let i = 0; i < len; i++) {
		const cc = str.charCodeAt(i);
		if (cc >= CHAR_0 && cc <= CHAR_9 || cc === CHAR_MINUS) continue;
		if (cc < 1632) {
			if (MINUS_SET.has(cc)) {
				firstHit = i;
				break;
			}
			continue;
		}
		if (cc >= 55296 && cc <= 56319) {
			if (i + 1 < len) {
				const low = str.charCodeAt(i + 1);
				if (low >= 56320 && low <= 57343) {
					const cp = 65536 + (cc - 55296 << 10) + (low - 56320);
					if (HIGH_MAP.has(cp)) {
						firstHit = i;
						break;
					}
				}
			}
			continue;
		}
		if (TABLE[cc - 1632] !== 255 || MINUS_SET.has(cc)) {
			firstHit = i;
			break;
		}
	}
	if (firstHit === -1) return str;
	const chars = [];
	if (firstHit > 0) chars.push(str.slice(0, firstHit));
	for (let i = firstHit; i < len; i++) {
		const cc = str.charCodeAt(i);
		if (cc >= CHAR_0 && cc <= CHAR_9 || cc === CHAR_MINUS) {
			chars.push(str[i]);
			continue;
		}
		if (cc < 1632) {
			chars.push(MINUS_SET.has(cc) ? "-" : str[i]);
			continue;
		}
		if (cc >= 55296 && cc <= 56319) {
			if (i + 1 < len) {
				const low = str.charCodeAt(i + 1);
				if (low >= 56320 && low <= 57343) {
					const cp = 65536 + (cc - 55296 << 10) + (low - 56320);
					const d = HIGH_MAP.get(cp);
					if (d !== void 0) {
						chars.push(String.fromCharCode(d + 48));
						i++;
						continue;
					}
				}
			}
			chars.push(str[i]);
			continue;
		}
		if (MINUS_SET.has(cc)) {
			chars.push("-");
			continue;
		}
		const d = TABLE[cc - TABLE_OFFSET];
		chars.push(d !== 255 ? String.fromCharCode(d + 48) : str[i]);
	}
	return chars.join("");
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/strnum@2.4.2/node_modules/strnum/strnum.js
var hexRegex = /^[-+]?0x[a-fA-F0-9]+$/;
var binRegex = /^0b[01]+$/;
var octRegex = /^0o[0-7]+$/;
var numRegex = /^([\-\+])?(0*)([0-9]*(\.[0-9]*)?)$/;
var consider = {
	hex: true,
	binary: false,
	octal: false,
	leadingZeros: true,
	decimalPoint: ".",
	eNotation: true,
	infinity: "original",
	unicode: false
};
function toNumber(str, options = {}) {
	options = Object.assign({}, consider, options);
	if (!str || typeof str !== "string") return str;
	let trimmedStr = str.trim();
	if (trimmedStr.length === 0) return str;
	else if (options.skipLike !== void 0 && options.skipLike.test(trimmedStr)) return str;
	else if (trimmedStr === "0") return 0;
	if (options.unicode) {
		trimmedStr = anynum(trimmedStr);
		if (trimmedStr === "0") return 0;
	}
	if (options.hex && hexRegex.test(trimmedStr)) return parse_int(trimmedStr, 16);
	else if (options.binary && binRegex.test(trimmedStr)) return parse_int(trimmedStr, 2);
	else if (options.octal && octRegex.test(trimmedStr)) return parse_int(trimmedStr, 8);
	else if (!isFinite(trimmedStr)) return handleInfinity(str, Number(trimmedStr), options);
	else if (trimmedStr.includes("e") || trimmedStr.includes("E")) return resolveEnotation(str, trimmedStr, options);
	else {
		const match = numRegex.exec(trimmedStr);
		if (match) {
			const sign = match[1] || "";
			const leadingZeros = match[2];
			let numTrimmedByZeros = trimZeros(match[3]);
			const decimalAdjacentToLeadingZeros = sign ? str[leadingZeros.length + 1] === "." : str[leadingZeros.length] === ".";
			if (!options.leadingZeros && (leadingZeros.length > 1 || leadingZeros.length === 1 && !decimalAdjacentToLeadingZeros)) return str;
			else {
				const num = Number(trimmedStr);
				const parsedStr = String(num);
				if (num === 0) return num;
				if (parsedStr.search(/[eE]/) !== -1) if (options.eNotation) return num;
				else return str;
				else if (trimmedStr.indexOf(".") !== -1) if (parsedStr === "0") return num;
				else if (parsedStr === numTrimmedByZeros) return num;
				else if (parsedStr === `${sign}${numTrimmedByZeros}`) return num;
				else return str;
				let n = leadingZeros ? numTrimmedByZeros : trimmedStr;
				if (leadingZeros) return n === parsedStr || sign + n === parsedStr ? num : str;
				else return n === parsedStr || n === sign + parsedStr ? num : str;
			}
		} else return str;
	}
}
var eNotationRegx = /^([-+])?(0*)(\d*(\.\d*)?[eE][-\+]?\d+)$/;
function resolveEnotation(str, trimmedStr, options) {
	if (!options.eNotation) return str;
	const notation = trimmedStr.match(eNotationRegx);
	if (notation) {
		let sign = notation[1] || "";
		const eChar = notation[3].indexOf("e") === -1 ? "E" : "e";
		const leadingZeros = notation[2];
		const eAdjacentToLeadingZeros = sign ? str[leadingZeros.length + 1] === eChar : str[leadingZeros.length] === eChar;
		if (leadingZeros.length > 1 && eAdjacentToLeadingZeros) return str;
		else if (leadingZeros.length === 1 && (notation[3].startsWith(`.${eChar}`) || notation[3][0] === eChar)) return Number(trimmedStr);
		else if (leadingZeros.length > 0) if (options.leadingZeros && !eAdjacentToLeadingZeros) {
			trimmedStr = (notation[1] || "") + notation[3];
			return Number(trimmedStr);
		} else return str;
		else return Number(trimmedStr);
	} else return str;
}
/**
* 
* @param {string} numStr without leading zeros
* @returns 
*/
function trimZeros(numStr) {
	if (numStr && numStr.indexOf(".") !== -1) {
		let end = numStr.length;
		while (end > 0 && numStr.charCodeAt(end - 1) === 48) end--;
		numStr = numStr.slice(0, end);
		if (numStr === ".") numStr = "0";
		else if (numStr[0] === ".") numStr = "0" + numStr;
		else if (numStr[numStr.length - 1] === ".") numStr = numStr.substring(0, numStr.length - 1);
		return numStr;
	}
	return numStr;
}
function parse_int(numStr, base) {
	const str = numStr.trim();
	if (base === 2 || base === 8) numStr = str.substring(2);
	if (parseInt) return parseInt(numStr, base);
	else if (Number.parseInt) return Number.parseInt(numStr, base);
	else if (window && window.parseInt) return window.parseInt(numStr, base);
	else throw new Error("parseInt, Number.parseInt, window.parseInt are not supported");
}
/**
* Handle infinite values based on user option
* @param {string} str - original input string
* @param {number} num - parsed number (Infinity or -Infinity)
* @param {object} options - user options
* @returns {string|number|null} based on infinity option
*/
function handleInfinity(str, num, options) {
	const isPositive = num === Infinity;
	switch (options.infinity.toLowerCase()) {
		case "null": return null;
		case "infinity": return num;
		case "string": return isPositive ? "Infinity" : "-Infinity";
		default: return str;
	}
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/ignoreAttributes.js
function getIgnoreAttributesFn(ignoreAttributes) {
	if (typeof ignoreAttributes === "function") return ignoreAttributes;
	if (Array.isArray(ignoreAttributes)) return (attrName) => {
		for (const pattern of ignoreAttributes) {
			if (typeof pattern === "string" && attrName === pattern) return true;
			if (pattern instanceof RegExp && pattern.test(attrName)) return true;
		}
	};
	return () => false;
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/path-expression-matcher@1.6.2/node_modules/path-expression-matcher/src/Expression.js
/**
* Expression - Parses and stores a tag pattern expression
* 
* Patterns are parsed once and stored in an optimized structure for fast matching.
* 
* @example
* const expr = new Expression("root.users.user");
* const expr2 = new Expression("..user[id]:first");
* const expr3 = new Expression("root/users/user", { separator: '/' });
*/
var Expression = class {
	/**
	* Create a new Expression
	* @param {string} pattern - Pattern string (e.g., "root.users.user", "..user[id]")
	* @param {Object} options - Configuration options
	* @param {string} options.separator - Path separator (default: '.')
	*/
	constructor(pattern, options = {}, data) {
		this.pattern = pattern;
		this.separator = options.separator || ".";
		this.segments = this._parse(pattern);
		this.data = data;
		this._hasDeepWildcard = this.segments.some((seg) => seg.type === "deep-wildcard");
		this._hasAttributeCondition = this.segments.some((seg) => seg.attrName !== void 0);
		this._hasPositionSelector = this.segments.some((seg) => seg.position !== void 0);
	}
	/**
	* Parse pattern string into segments
	* @private
	* @param {string} pattern - Pattern to parse
	* @returns {Array} Array of segment objects
	*/
	_parse(pattern) {
		const segments = [];
		let i = 0;
		let currentPart = "";
		while (i < pattern.length) if (pattern[i] === this.separator) if (i + 1 < pattern.length && pattern[i + 1] === this.separator) {
			if (currentPart.trim()) {
				segments.push(this._parseSegment(currentPart.trim()));
				currentPart = "";
			}
			segments.push({ type: "deep-wildcard" });
			i += 2;
		} else {
			if (currentPart.trim()) segments.push(this._parseSegment(currentPart.trim()));
			currentPart = "";
			i++;
		}
		else {
			currentPart += pattern[i];
			i++;
		}
		if (currentPart.trim()) segments.push(this._parseSegment(currentPart.trim()));
		return segments;
	}
	/**
	* Parse a single segment
	* @private
	* @param {string} part - Segment string (e.g., "user", "ns::user", "user[id]", "ns::user:first")
	* @returns {Object} Segment object
	*/
	_parseSegment(part) {
		const segment = { type: "tag" };
		let bracketContent = null;
		let withoutBrackets = part;
		const bracketMatch = part.match(/^([^\[]+)(\[[^\]]*\])(.*)$/);
		if (bracketMatch) {
			withoutBrackets = bracketMatch[1] + bracketMatch[3];
			if (bracketMatch[2]) {
				const content = bracketMatch[2].slice(1, -1);
				if (content) bracketContent = content;
			}
		}
		let namespace = void 0;
		let tagAndPosition = withoutBrackets;
		if (withoutBrackets.includes("::")) {
			const nsIndex = withoutBrackets.indexOf("::");
			namespace = withoutBrackets.substring(0, nsIndex).trim();
			tagAndPosition = withoutBrackets.substring(nsIndex + 2).trim();
			if (!namespace) throw new Error(`Invalid namespace in pattern: ${part}`);
		}
		let tag = void 0;
		let positionMatch = null;
		if (tagAndPosition.includes(":")) {
			const colonIndex = tagAndPosition.lastIndexOf(":");
			const tagPart = tagAndPosition.substring(0, colonIndex).trim();
			const posPart = tagAndPosition.substring(colonIndex + 1).trim();
			if ([
				"first",
				"last",
				"odd",
				"even"
			].includes(posPart) || /^nth\(\d+\)$/.test(posPart)) {
				tag = tagPart;
				positionMatch = posPart;
			} else tag = tagAndPosition;
		} else tag = tagAndPosition;
		if (!tag) throw new Error(`Invalid segment pattern: ${part}`);
		segment.tag = tag;
		if (namespace) segment.namespace = namespace;
		if (bracketContent) if (bracketContent.includes("=")) {
			const eqIndex = bracketContent.indexOf("=");
			segment.attrName = bracketContent.substring(0, eqIndex).trim();
			segment.attrValue = bracketContent.substring(eqIndex + 1).trim();
		} else segment.attrName = bracketContent.trim();
		if (positionMatch) {
			const nthMatch = positionMatch.match(/^nth\((\d+)\)$/);
			if (nthMatch) {
				segment.position = "nth";
				segment.positionValue = parseInt(nthMatch[1], 10);
			} else segment.position = positionMatch;
		}
		return segment;
	}
	/**
	* Get the number of segments
	* @returns {number}
	*/
	get length() {
		return this.segments.length;
	}
	/**
	* Check if expression contains deep wildcard
	* @returns {boolean}
	*/
	hasDeepWildcard() {
		return this._hasDeepWildcard;
	}
	/**
	* Check if expression has attribute conditions
	* @returns {boolean}
	*/
	hasAttributeCondition() {
		return this._hasAttributeCondition;
	}
	/**
	* Check if expression has position selectors
	* @returns {boolean}
	*/
	hasPositionSelector() {
		return this._hasPositionSelector;
	}
	/**
	* Get string representation
	* @returns {string}
	*/
	toString() {
		return this.pattern;
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/path-expression-matcher@1.6.2/node_modules/path-expression-matcher/src/ExpressionSet.js
/**
* ExpressionSet - An indexed collection of Expressions for efficient bulk matching
*
* Instead of iterating all expressions on every tag, ExpressionSet pre-indexes
* them at insertion time by depth and terminal tag name. At match time, only
* the relevant bucket is evaluated — typically reducing checks from O(E) to O(1)
* lookup plus O(small bucket) matches.
*
* Three buckets are maintained:
*  - `_byDepthAndTag`  — exact depth + exact tag name  (tightest, used first)
*  - `_wildcardByDepth` — exact depth + wildcard tag `*` (depth-matched only)
*  - `_deepWildcards`  — expressions containing `..`  (cannot be depth-indexed)
*
* @example
* import { Expression, ExpressionSet } from 'fast-xml-tagger';
*
* // Build once at config time
* const stopNodes = new ExpressionSet();
* stopNodes.add(new Expression('root.users.user'));
* stopNodes.add(new Expression('root.config.setting'));
* stopNodes.add(new Expression('..script'));
*
* // Query on every tag — hot path
* if (stopNodes.matchesAny(matcher)) { ... }
*/
var ExpressionSet = class {
	constructor() {
		/** @type {Map<string, import('./Expression.js').default[]>} depth:tag → expressions */
		this._byDepthAndTag = /* @__PURE__ */ new Map();
		/** @type {Map<number, import('./Expression.js').default[]>} depth → wildcard-tag expressions */
		this._wildcardByDepth = /* @__PURE__ */ new Map();
		/** @type {import('./Expression.js').default[]} expressions containing deep wildcard (..) */
		this._deepWildcards = [];
		/** @type {Map<string, import('./Expression.js').default[]>} terminalTag → deep wildcard expressions */
		this._deepByTerminalTag = /* @__PURE__ */ new Map();
		/** @type {Set<string>} pattern strings already added — used for deduplication */
		this._patterns = /* @__PURE__ */ new Set();
		/** @type {boolean} whether the set is sealed against further additions */
		this._sealed = false;
	}
	/**
	* Add an Expression to the set.
	* Duplicate patterns (same pattern string) are silently ignored.
	*
	* @param {import('./Expression.js').default} expression - A pre-constructed Expression instance
	* @returns {this} for chaining
	* @throws {TypeError} if called after seal()
	*
	* @example
	* set.add(new Expression('root.users.user'));
	* set.add(new Expression('..script'));
	*/
	add(expression) {
		if (this._sealed) throw new TypeError("ExpressionSet is sealed. Create a new ExpressionSet to add more expressions.");
		if (this._patterns.has(expression.pattern)) return this;
		this._patterns.add(expression.pattern);
		if (expression.hasDeepWildcard()) {
			const lastSeg = expression.segments[expression.segments.length - 1];
			if (lastSeg && lastSeg.type !== "deep-wildcard" && lastSeg.tag !== "*") {
				const tag = lastSeg.tag;
				if (!this._deepByTerminalTag.has(tag)) this._deepByTerminalTag.set(tag, []);
				this._deepByTerminalTag.get(tag).push(expression);
			} else this._deepWildcards.push(expression);
			return this;
		}
		const depth = expression.length;
		const tag = expression.segments[expression.segments.length - 1]?.tag;
		if (!tag || tag === "*") {
			if (!this._wildcardByDepth.has(depth)) this._wildcardByDepth.set(depth, []);
			this._wildcardByDepth.get(depth).push(expression);
		} else {
			const key = `${depth}:${tag}`;
			if (!this._byDepthAndTag.has(key)) this._byDepthAndTag.set(key, []);
			this._byDepthAndTag.get(key).push(expression);
		}
		return this;
	}
	/**
	* Add multiple expressions at once.
	*
	* @param {import('./Expression.js').default[]} expressions - Array of Expression instances
	* @returns {this} for chaining
	*
	* @example
	* set.addAll([
	*   new Expression('root.users.user'),
	*   new Expression('root.config.setting'),
	* ]);
	*/
	addAll(expressions) {
		for (const expr of expressions) this.add(expr);
		return this;
	}
	/**
	* Check whether a pattern string is already present in the set.
	*
	* @param {import('./Expression.js').default} expression
	* @returns {boolean}
	*/
	has(expression) {
		return this._patterns.has(expression.pattern);
	}
	/**
	* Number of expressions in the set.
	* @type {number}
	*/
	get size() {
		return this._patterns.size;
	}
	/**
	* Seal the set against further modifications.
	* Useful to prevent accidental mutations after config is built.
	* Calling add() or addAll() on a sealed set throws a TypeError.
	*
	* @returns {this}
	*/
	seal() {
		this._sealed = true;
		return this;
	}
	/**
	* Whether the set has been sealed.
	* @type {boolean}
	*/
	get isSealed() {
		return this._sealed;
	}
	/**
	* Test whether the matcher's current path matches any expression in the set.
	*
	* Evaluation order (cheapest → most expensive):
	*  1. Exact depth + tag bucket  — O(1) lookup, typically 0–2 expressions
	*  2. Depth-only wildcard bucket — O(1) lookup, rare
	*  3. Deep-wildcard list         — always checked, but usually small
	*
	* @param {import('./Matcher.js').default} matcher - Matcher instance (or readOnly view)
	* @returns {boolean} true if any expression matches the current path
	*
	* @example
	* if (stopNodes.matchesAny(matcher)) {
	*   // handle stop node
	* }
	*/
	matchesAny(matcher) {
		return this.findMatch(matcher) !== null;
	}
	/**
	* Find and return the first Expression that matches the matcher's current path.
	*
	* Uses the same evaluation order as matchesAny (cheapest → most expensive):
	*  1. Exact depth + tag bucket
	*  2. Depth-only wildcard bucket
	*  3. Deep-wildcard list
	*
	* @param {import('./Matcher.js').default} matcher - Matcher instance (or readOnly view)
	* @returns {import('./Expression.js').default | null} the first matching Expression, or null
	*
	* @example
	* const expr = stopNodes.findMatch(matcher);
	* if (expr) {
	*   // access expr.config, expr.pattern, etc.
	* }
	*/
	findMatch(matcher) {
		const depth = matcher.getDepth();
		const tag = matcher.getCurrentTag();
		const exactKey = `${depth}:${tag}`;
		const exactBucket = this._byDepthAndTag.get(exactKey);
		if (exactBucket) {
			for (let i = 0; i < exactBucket.length; i++) if (matcher.matches(exactBucket[i])) return exactBucket[i];
		}
		const wildcardBucket = this._wildcardByDepth.get(depth);
		if (wildcardBucket) {
			for (let i = 0; i < wildcardBucket.length; i++) if (matcher.matches(wildcardBucket[i])) return wildcardBucket[i];
		}
		const deepBucket = this._deepByTerminalTag.get(tag);
		if (deepBucket) {
			for (let i = 0; i < deepBucket.length; i++) if (matcher.matches(deepBucket[i])) return deepBucket[i];
		}
		for (let i = 0; i < this._deepWildcards.length; i++) if (matcher.matches(this._deepWildcards[i])) return this._deepWildcards[i];
		return null;
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/path-expression-matcher@1.6.2/node_modules/path-expression-matcher/src/Matcher.js
/**
* MatcherView - A lightweight read-only view over a Matcher's internal state.
*
* Created once by Matcher and reused across all callbacks. Holds a direct
* reference to the parent Matcher so it always reflects current parser state
* with zero copying or freezing overhead.
*
* Users receive this via {@link Matcher#readOnly} or directly from parser
* callbacks. It exposes all query and matching methods but has no mutation
* methods — misuse is caught at the TypeScript level rather than at runtime.
*
* @example
* const matcher = new Matcher();
* const view = matcher.readOnly();
*
* matcher.push("root", {});
* view.getCurrentTag(); // "root"
* view.getDepth();      // 1
*/
var MatcherView = class {
	/**
	* @param {Matcher} matcher - The parent Matcher instance to read from.
	*/
	constructor(matcher) {
		this._matcher = matcher;
	}
	/**
	* Get the path separator used by the parent matcher.
	* @returns {string}
	*/
	get separator() {
		return this._matcher.separator;
	}
	/**
	* Get current tag name.
	* @returns {string|undefined}
	*/
	getCurrentTag() {
		const path = this._matcher.path;
		return path.length > 0 ? path[path.length - 1].tag : void 0;
	}
	/**
	* Get current namespace.
	* @returns {string|undefined}
	*/
	getCurrentNamespace() {
		const path = this._matcher.path;
		return path.length > 0 ? path[path.length - 1].namespace : void 0;
	}
	/**
	* Get current node's attribute value.
	* @param {string} attrName
	* @returns {*}
	*/
	getAttrValue(attrName) {
		const path = this._matcher.path;
		if (path.length === 0) return void 0;
		return path[path.length - 1].values?.[attrName];
	}
	/**
	* Check if current node has an attribute.
	* @param {string} attrName
	* @returns {boolean}
	*/
	hasAttr(attrName) {
		const path = this._matcher.path;
		if (path.length === 0) return false;
		const current = path[path.length - 1];
		return current.values !== void 0 && attrName in current.values;
	}
	/**
	* Get the value of a "kept" attribute from the nearest ancestor (or
	* current node) that declared it via `push(tag, attrs, ns, { keep: [...] })`.
	* @param {string} attrName
	* @returns {*}
	*/
	getAnyParentAttr(attrName) {
		return this._matcher.getAnyParentAttr(attrName);
	}
	/**
	* Check whether any ancestor (or the current node) kept the given
	* attribute via `push(tag, attrs, ns, { keep: [...] })`.
	* @param {string} attrName
	* @returns {boolean}
	*/
	hasAnyParentAttr(attrName) {
		return this._matcher.hasAnyParentAttr(attrName);
	}
	/**
	* Get current node's sibling position (child index in parent).
	* @returns {number}
	*/
	getPosition() {
		const path = this._matcher.path;
		if (path.length === 0) return -1;
		return path[path.length - 1].position ?? 0;
	}
	/**
	* Get current node's repeat counter (occurrence count of this tag name).
	* @returns {number}
	*/
	getCounter() {
		const path = this._matcher.path;
		if (path.length === 0) return -1;
		return path[path.length - 1].counter ?? 0;
	}
	/**
	* Get current node's sibling index (alias for getPosition).
	* @returns {number}
	* @deprecated Use getPosition() or getCounter() instead
	*/
	getIndex() {
		return this.getPosition();
	}
	/**
	* Get current path depth.
	* @returns {number}
	*/
	getDepth() {
		return this._matcher.path.length;
	}
	/**
	* Get path as string.
	* @param {string} [separator] - Optional separator (uses default if not provided)
	* @param {boolean} [includeNamespace=true]
	* @returns {string}
	*/
	toString(separator, includeNamespace = true) {
		return this._matcher.toString(separator, includeNamespace);
	}
	/**
	* Get path as array of tag names.
	* @returns {string[]}
	*/
	toArray() {
		return this._matcher.path.map((n) => n.tag);
	}
	/**
	* Match current path against an Expression.
	* @param {Expression} expression
	* @returns {boolean}
	*/
	matches(expression) {
		return this._matcher.matches(expression);
	}
	/**
	* Match any expression in the given set against the current path.
	* @param {ExpressionSet} exprSet
	* @returns {boolean}
	*/
	matchesAny(exprSet) {
		return exprSet.matchesAny(this._matcher);
	}
};
/**
* Matcher - Tracks current path in XML/JSON tree and matches against Expressions.
*
* The matcher maintains a stack of nodes representing the current path from root to
* current tag. It only stores attribute values for the current (top) node to minimize
* memory usage. Sibling tracking is used to auto-calculate position and counter.
*
* Use {@link Matcher#readOnly} to obtain a {@link MatcherView} safe to pass to
* user callbacks — it always reflects current state with no Proxy overhead.
*
* @example
* const matcher = new Matcher();
* matcher.push("root", {});
* matcher.push("users", {});
* matcher.push("user", { id: "123", type: "admin" });
*
* const expr = new Expression("root.users.user");
* matcher.matches(expr); // true
*/
var Matcher = class {
	/**
	* Create a new Matcher.
	* @param {Object} [options={}]
	* @param {string} [options.separator='.'] - Default path separator
	*/
	constructor(options = {}) {
		this.separator = options.separator || ".";
		this.path = [];
		this.siblingStacks = [];
		this._pathStringCache = null;
		this._view = new MatcherView(this);
		this._keptAttrs = [];
	}
	/**
	* Push a new tag onto the path.
	* @param {string} tagName
	* @param {Object|null} [attrValues=null]
	* @param {string|null} [namespace=null]
	* @param {Object|null} [options=null]
	* @param {string[]} [options.keep] - Names of attributes (from attrValues)
	*/
	push(tagName, attrValues = null, namespace = null, options = null) {
		this._pathStringCache = null;
		if (this.path.length > 0) this.path[this.path.length - 1].values = void 0;
		const currentLevel = this.path.length;
		let level = this.siblingStacks[currentLevel];
		if (!level) {
			level = {
				counts: /* @__PURE__ */ new Map(),
				total: 0
			};
			this.siblingStacks[currentLevel] = level;
		}
		const siblingKey = namespace ? `${namespace}:${tagName}` : tagName;
		const counter = level.counts.get(siblingKey) || 0;
		const position = level.total;
		level.counts.set(siblingKey, counter + 1);
		level.total++;
		const node = {
			tag: tagName,
			position,
			counter
		};
		if (namespace !== null && namespace !== void 0) node.namespace = namespace;
		if (attrValues !== null && attrValues !== void 0) node.values = attrValues;
		this.path.push(node);
		const depth = this.path.length;
		const keep = options !== null ? options.keep : null;
		if (keep !== null && keep !== void 0 && keep.length > 0 && attrValues) for (let i = 0; i < keep.length; i++) {
			const name = keep[i];
			if (attrValues[name] !== void 0) this._keptAttrs.push({
				depth,
				name,
				value: attrValues[name]
			});
		}
	}
	/**
	* Pop the last tag from the path.
	* @returns {Object|undefined} The popped node
	*/
	pop() {
		if (this.path.length === 0) return void 0;
		this._pathStringCache = null;
		const node = this.path.pop();
		if (this.siblingStacks.length > this.path.length + 1) this.siblingStacks.length = this.path.length + 1;
		const poppedDepth = this.path.length + 1;
		while (this._keptAttrs.length > 0 && this._keptAttrs[this._keptAttrs.length - 1].depth >= poppedDepth) this._keptAttrs.pop();
		return node;
	}
	/**
	* Update current node's attribute values.
	* Useful when attributes are parsed after push.
	* @param {Object} attrValues
	*/
	updateCurrent(attrValues) {
		if (this.path.length > 0) {
			const current = this.path[this.path.length - 1];
			if (attrValues !== null && attrValues !== void 0) current.values = attrValues;
		}
	}
	/**
	* Get current tag name.
	* @returns {string|undefined}
	*/
	getCurrentTag() {
		return this.path.length > 0 ? this.path[this.path.length - 1].tag : void 0;
	}
	/**
	* Get current namespace.
	* @returns {string|undefined}
	*/
	getCurrentNamespace() {
		return this.path.length > 0 ? this.path[this.path.length - 1].namespace : void 0;
	}
	/**
	* Get current node's attribute value.
	* @param {string} attrName
	* @returns {*}
	*/
	getAttrValue(attrName) {
		if (this.path.length === 0) return void 0;
		return this.path[this.path.length - 1].values?.[attrName];
	}
	/**
	* Check if current node has an attribute.
	* @param {string} attrName
	* @returns {boolean}
	*/
	hasAttr(attrName) {
		if (this.path.length === 0) return false;
		const current = this.path[this.path.length - 1];
		return current.values !== void 0 && attrName in current.values;
	}
	/**
	* Get the value of a "kept" attribute from the nearest ancestor (or
	* current node) that declared it via `push(tag, attrs, ns, { keep: [...] })`.
	* Unlike getAttrValue(), this works regardless of how deep the path has
	* gone since the attribute was pushed — but only for attribute names that
	* were explicitly marked with `keep` at push time. Cost is proportional to
	* the number of currently-kept attributes (typically 0-3), not path depth.
	* @param {string} attrName
	* @returns {*} the value, or undefined if no ancestor kept this attribute
	*/
	getAnyParentAttr(attrName) {
		const kept = this._keptAttrs;
		for (let i = kept.length - 1; i >= 0; i--) if (kept[i].name === attrName) return kept[i].value;
	}
	/**
	* Check whether any ancestor (or the current node) kept the given
	* attribute via `push(tag, attrs, ns, { keep: [...] })`.
	* @param {string} attrName
	* @returns {boolean}
	*/
	hasAnyParentAttr(attrName) {
		const kept = this._keptAttrs;
		for (let i = kept.length - 1; i >= 0; i--) if (kept[i].name === attrName) return true;
		return false;
	}
	/**
	* Get current node's sibling position (child index in parent).
	* @returns {number}
	*/
	getPosition() {
		if (this.path.length === 0) return -1;
		return this.path[this.path.length - 1].position ?? 0;
	}
	/**
	* Get current node's repeat counter (occurrence count of this tag name).
	* @returns {number}
	*/
	getCounter() {
		if (this.path.length === 0) return -1;
		return this.path[this.path.length - 1].counter ?? 0;
	}
	/**
	* Get current node's sibling index (alias for getPosition).
	* @returns {number}
	* @deprecated Use getPosition() or getCounter() instead
	*/
	getIndex() {
		return this.getPosition();
	}
	/**
	* Get current path depth.
	* @returns {number}
	*/
	getDepth() {
		return this.path.length;
	}
	/**
	* Get path as string.
	* @param {string} [separator] - Optional separator (uses default if not provided)
	* @param {boolean} [includeNamespace=true]
	* @returns {string}
	*/
	toString(separator, includeNamespace = true) {
		const sep = separator || this.separator;
		if (sep === this.separator && includeNamespace === true) {
			if (this._pathStringCache !== null) return this._pathStringCache;
			const result = this.path.map((n) => n.namespace ? `${n.namespace}:${n.tag}` : n.tag).join(sep);
			this._pathStringCache = result;
			return result;
		}
		return this.path.map((n) => includeNamespace && n.namespace ? `${n.namespace}:${n.tag}` : n.tag).join(sep);
	}
	/**
	* Get path as array of tag names.
	* @returns {string[]}
	*/
	toArray() {
		return this.path.map((n) => n.tag);
	}
	/**
	* Reset the path to empty.
	*/
	reset() {
		this._pathStringCache = null;
		this.path = [];
		this.siblingStacks = [];
		this._keptAttrs = [];
	}
	/**
	* Match current path against an Expression.
	* @param {Expression} expression
	* @returns {boolean}
	*/
	matches(expression) {
		const segments = expression.segments;
		if (segments.length === 0) return false;
		if (expression.hasDeepWildcard()) return this._matchWithDeepWildcard(segments);
		return this._matchSimple(segments);
	}
	/**
	* @private
	*/
	_matchSimple(segments) {
		if (this.path.length !== segments.length) return false;
		for (let i = 0; i < segments.length; i++) if (!this._matchSegment(segments[i], this.path[i], i === this.path.length - 1)) return false;
		return true;
	}
	/**
	* @private
	*/
	_matchWithDeepWildcard(segments) {
		let pathIdx = this.path.length - 1;
		let segIdx = segments.length - 1;
		while (segIdx >= 0 && pathIdx >= 0) {
			const segment = segments[segIdx];
			if (segment.type === "deep-wildcard") {
				segIdx--;
				if (segIdx < 0) return true;
				const nextSeg = segments[segIdx];
				let found = false;
				for (let i = pathIdx; i >= 0; i--) if (this._matchSegment(nextSeg, this.path[i], i === this.path.length - 1)) {
					pathIdx = i - 1;
					segIdx--;
					found = true;
					break;
				}
				if (!found) return false;
			} else {
				if (!this._matchSegment(segment, this.path[pathIdx], pathIdx === this.path.length - 1)) return false;
				pathIdx--;
				segIdx--;
			}
		}
		return segIdx < 0;
	}
	/**
	* @private
	*/
	_matchSegment(segment, node, isCurrentNode) {
		if (segment.tag !== "*" && segment.tag !== node.tag) return false;
		if (segment.namespace !== void 0) {
			if (segment.namespace !== "*" && segment.namespace !== node.namespace) return false;
		}
		if (segment.attrName !== void 0) {
			if (!isCurrentNode) return false;
			if (!node.values || !(segment.attrName in node.values)) return false;
			if (segment.attrValue !== void 0) {
				if (String(node.values[segment.attrName]) !== String(segment.attrValue)) return false;
			}
		}
		if (segment.position !== void 0) {
			if (!isCurrentNode) return false;
			const counter = node.counter ?? 0;
			if (segment.position === "first" && counter !== 0) return false;
			else if (segment.position === "odd" && counter % 2 !== 1) return false;
			else if (segment.position === "even" && counter % 2 !== 0) return false;
			else if (segment.position === "nth" && counter !== segment.positionValue) return false;
		}
		return true;
	}
	/**
	* Match any expression in the given set against the current path.
	* @param {ExpressionSet} exprSet
	* @returns {boolean}
	*/
	matchesAny(exprSet) {
		return exprSet.matchesAny(this);
	}
	/**
	* Create a snapshot of current state.
	* @returns {Object}
	*/
	snapshot() {
		return {
			path: this.path.map((node) => ({ ...node })),
			siblingStacks: this.siblingStacks.map((level) => level ? {
				counts: new Map(level.counts),
				total: level.total
			} : level),
			keptAttrs: this._keptAttrs.map((entry) => ({ ...entry }))
		};
	}
	/**
	* Restore state from snapshot.
	* @param {Object} snapshot
	*/
	restore(snapshot) {
		this._pathStringCache = null;
		this.path = snapshot.path.map((node) => ({ ...node }));
		this.siblingStacks = snapshot.siblingStacks.map((level) => level ? {
			counts: new Map(level.counts),
			total: level.total
		} : level);
		this._keptAttrs = (snapshot.keptAttrs || []).map((entry) => ({ ...entry }));
	}
	/**
	* Return the read-only {@link MatcherView} for this matcher.
	*
	* The same instance is returned on every call — no allocation occurs.
	* It always reflects the current parser state and is safe to pass to
	* user callbacks without risk of accidental mutation.
	*
	* @returns {MatcherView}
	*
	* @example
	* const view = matcher.readOnly();
	* // pass view to callbacks — it stays in sync automatically
	* view.matches(expr);       // ✓
	* view.getCurrentTag();     // ✓
	* // view.push(...)         // ✗ method does not exist — caught by TypeScript
	*/
	readOnly() {
		return this._view;
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/html.js
/**
* HTML context patterns.
*
* Detects XSS vectors that are dangerous when a string ends up rendered as HTML.
* All patterns use bounded quantifiers to ensure linear-time matching (ReDoS-safe).
*
* Each entry is { pattern: RegExp, id: string, description: string }
* so callers can inspect which rule fired if they need to.
*/
var HTML_PATTERNS = [
	{
		id: "html-script-open",
		description: "<script opening tag",
		pattern: /<script[\s>/]/i
	},
	{
		id: "html-script-close",
		description: "<\/script closing tag",
		pattern: /<\/script[\s>]/i
	},
	{
		id: "html-javascript-protocol",
		description: "javascript: URI scheme (with optional whitespace/encoding)",
		pattern: /j[\t\n\r ]*a[\t\n\r ]*v[\t\n\r ]*a[\t\n\r ]*s[\t\n\r ]*c[\t\n\r ]*r[\t\n\r ]*i[\t\n\r ]*p[\t\n\r ]*t[\t\n\r ]*:/i
	},
	{
		id: "html-vbscript-protocol",
		description: "vbscript: URI scheme",
		pattern: /vbscript[\t\n\r ]*:/i
	},
	{
		id: "html-data-html",
		description: "data:text/html URI — can execute scripts in browsers",
		pattern: /data[\t\n\r ]*:[\t\n\r ]*text\/html/i
	},
	{
		id: "html-data-xhtml",
		description: "data:application/xhtml+xml URI",
		pattern: /data[\t\n\r ]*:[\t\n\r ]*application\/xhtml/i
	},
	{
		id: "html-data-svg",
		description: "data:image/svg+xml URI — can execute scripts",
		pattern: /data[\t\n\r ]*:[\t\n\r ]*image\/svg\+xml/i
	},
	{
		id: "html-inline-event-handler",
		description: "Inline event handler attributes: onclick=, onerror=, onload=, etc.",
		pattern: /\bon\w{1,30}\s*=/i
	},
	{
		id: "html-entity-obfuscated-script",
		description: "HTML-entity-encoded <script (e.g. &#x3C;script or &lt;script)",
		pattern: /(?:&#x0*3[Cc];?|&#0*60;?|&lt;)\s*script/i
	},
	{
		id: "html-entity-obfuscated-javascript",
		description: "HTML-entity-encoded javascript: (partial — catches common &#106; or &#x6a; for \"j\")",
		pattern: /(?:&#x0*6[Aa];?|&#0*106;?)\s*(?:&#x0*61;?|a)[\s\S]{0,80}script\s*:/i
	},
	{
		id: "html-style-expression",
		description: "CSS expression() — IE-era code execution in style attributes",
		pattern: /style[\s\S]{0,20}expression\s*\(/i
	},
	{
		id: "html-object-embed",
		description: "<object or <embed tags that can load active content",
		pattern: /<(?:object|embed)[\s>/]/i
	},
	{
		id: "html-base-tag",
		description: "<base href= — can hijack all relative URLs on a page",
		pattern: /<base[\s>]/i
	},
	{
		id: "html-meta-refresh",
		description: "<meta http-equiv=\"refresh\" — can redirect users",
		pattern: /<meta[\s\S]{0,40}http-equiv[\s\S]{0,20}refresh/i
	},
	{
		id: "html-srcdoc",
		description: "srcdoc= attribute on iframes — embeds HTML that can run scripts",
		pattern: /srcdoc\s*=/i
	},
	{
		id: "html-iframe",
		description: "<iframe tag",
		pattern: /<iframe[\s>/]/i
	},
	{
		id: "html-form",
		description: "<form tag — can be used for phishing / credential harvesting injection",
		pattern: /<form[\s>/]/i
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/xml.js
/**
* XML context patterns.
*
* Detects injection vectors that are specifically dangerous when a string
* is inserted into an XML document (not HTML rendering context).
*
* Key distinction from HTML: these patterns target parser-level attacks —
* things that can confuse or subvert an XML parser, trigger external entity
* resolution, or inject DTD content. HTML rendering concerns (XSS) belong
* in the HTML context.
*/
var XML_PATTERNS = [
	{
		id: "xml-cdata-injection",
		description: "CDATA section injection: <![CDATA[ breaks out of text node context",
		pattern: /<!\[CDATA\[/i
	},
	{
		id: "xml-cdata-close",
		description: "CDATA close sequence: ]]> can terminate an enclosing CDATA section",
		pattern: /\]\]>/
	},
	{
		id: "xml-processing-instruction",
		description: "XML processing instruction: <?xml-stylesheet or <?php etc.",
		pattern: /<\?(?:xml[\- ]|php|asp)/i
	},
	{
		id: "xml-doctype-injection",
		description: "DOCTYPE declaration embedded in content — can define entities",
		pattern: /<!DOCTYPE(?:[\s[]|$)/i
	},
	{
		id: "xml-entity-system",
		description: "SYSTEM keyword — used in external entity declarations (XXE)",
		pattern: /\bSYSTEM\s+["']/i
	},
	{
		id: "xml-entity-public",
		description: "PUBLIC keyword — used in external entity declarations (XXE)",
		pattern: /\bPUBLIC\s+["']/i
	},
	{
		id: "xml-entity-declaration",
		description: "<!ENTITY declaration — defines entities, potential XXE or entity expansion",
		pattern: /<!ENTITY[\s%]/i
	},
	{
		id: "xml-billion-laughs",
		description: "Entity reference chaining / billion laughs: repeated &eX; style references",
		pattern: /(?:&\w{1,20};){3,}/
	},
	{
		id: "xml-namespace-confusion",
		description: "xmlns: attribute injection — can redefine namespaces to confuse parsers",
		pattern: /\bxmlns(?::\w{1,40})?\s*=/i
	},
	{
		id: "xml-comment-injection",
		description: "<!-- comment injection — can hide content from some parsers",
		pattern: /<!--/
	},
	{
		id: "xml-comment-close",
		description: "--> closes an enclosing XML comment",
		pattern: /-->/
	},
	{
		id: "xml-pi-close",
		description: "?> closes an enclosing processing instruction",
		pattern: /\?>/
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/svg.js
/**
* SVG context patterns.
*
* SVG is XML-based but renders in browsers, giving it a unique attack surface
* that combines XML parser behaviour with browser rendering and JavaScript execution.
*
* Many of these vectors bypass HTML sanitizers that don't understand SVG semantics
* (DOMPurify has documented bypass vulnerabilities specifically in SVG/XML context).
*/
var SVG_PATTERNS = [
	{
		id: "svg-script-element",
		description: "<script element inside SVG executes JavaScript",
		pattern: /<script[\s>/]/i
	},
	{
		id: "svg-xlink-href-javascript",
		description: "xlink:href with javascript: — classic SVG XSS via <a> or <use>",
		pattern: /xlink\s*:\s*href\s*=\s*["']?\s*javascript\s*:/i
	},
	{
		id: "svg-href-javascript",
		description: "href= with javascript: in SVG context (<a>, <animate>, etc.)",
		pattern: /href\s*=\s*["']?\s*javascript\s*:/i
	},
	{
		id: "svg-foreignobject",
		description: "<foreignObject embeds HTML inside SVG — can execute scripts",
		pattern: /<foreignObject[\s>/]/i
	},
	{
		id: "svg-use-external",
		description: "<use xlink:href or href pointing to external resource (non-fragment URL)",
		pattern: /<use[\s\S]{0,60}(?:xlink\s*:\s*)?href\s*=\s*(?:["'][^#]|[^"'#\s>])/i
	},
	{
		id: "svg-animate-href",
		description: "<animate attributeName=\"href\" — can dynamically change href to javascript:",
		pattern: /<animate[\s\S]{0,80}attributeName\s*=\s*["'][\s]*href["']/i
	},
	{
		id: "svg-animate-xlinkhref",
		description: "<animate attributeName=\"xlink:href\"",
		pattern: /<animate[\s\S]{0,80}attributeName\s*=\s*["'][\s]*xlink\s*:\s*href["']/i
	},
	{
		id: "svg-set-javascript",
		description: "<set to=\"javascript:...\" — sets an attribute to a javascript: URI",
		pattern: /<set[\s\S]{0,80}to\s*=\s*["']?\s*javascript\s*:/i
	},
	{
		id: "svg-event-handler",
		description: "SVG-specific event handler attributes: onload=, onerror=, onactivate=, etc.",
		pattern: /\bon(?:load|error|activate|begin|end|repeat|focus|blur|click|mouse\w{1,20}|key\w{1,20})\s*=/i
	},
	{
		id: "svg-handler-generic",
		description: "Generic on* handler catch-all for SVG attributes",
		pattern: /\bon\w{1,30}\s*=/i
	},
	{
		id: "svg-filter-feimage",
		description: "<feImage href= — filter primitive that can load external resources",
		pattern: /<feImage[\s\S]{0,80}(?:xlink\s*:\s*)?href\s*=/i
	},
	{
		id: "svg-image-external",
		description: "<image xlink:href with http/https or javascript protocol",
		pattern: /<image[\s\S]{0,80}(?:xlink\s*:\s*)?href\s*=\s*["']?\s*(?:https?|javascript)\s*:/i
	},
	{
		id: "svg-style-javascript",
		description: "style= attribute containing javascript: (e.g. background:url(javascript:...))",
		pattern: /style\s*=[\s\S]{0,60}javascript\s*:/i
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/sql.js
/**
* SQL context patterns — high-precision rules only.
*
* These rules have very low false-positive risk and are safe to apply to
* general user text (names, descriptions, search queries, etc.).
* All patterns are ReDoS-safe — unlike the `sql-injection` npm package
* which has an active CVE on its own detection regexes.
*
* For exhaustive coverage including noisier heuristics (comment sequences,
* hex literals, stacked queries with semicolons), use 'SQL-STRICT' instead.
* Apply 'SQL-STRICT' only to strings that are specifically SQL fragments,
* not to general free-text fields.
*/
var SQL_PATTERNS = [
	{
		id: "sql-block-comment-open",
		description: "SQL block comment open: /* ... */ — unusual in legitimate user text",
		pattern: /\/\*/
	},
	{
		id: "sql-union-select",
		description: "UNION SELECT — most common SQL injection aggregation attack",
		pattern: /\bUNION\s{1,20}(?:ALL\s{1,20})?SELECT\b/i
	},
	{
		id: "sql-drop-table",
		description: "DROP TABLE — destructive DDL injection",
		pattern: /\bDROP\s{1,20}TABLE\b/i
	},
	{
		id: "sql-drop-database",
		description: "DROP DATABASE — destructive DDL injection",
		pattern: /\bDROP\s{1,20}DATABASE\b/i
	},
	{
		id: "sql-insert-into",
		description: "INSERT INTO — data injection",
		pattern: /\bINSERT\s{1,20}INTO\b/i
	},
	{
		id: "sql-delete-from",
		description: "DELETE FROM — data deletion injection",
		pattern: /\bDELETE\s{1,20}FROM\b/i
	},
	{
		id: "sql-update-set",
		description: "UPDATE ... SET — data modification injection",
		pattern: /\bUPDATE\b[\s\S]{1,60}\bSET\b/i
	},
	{
		id: "sql-exec-xp",
		description: "EXEC xp_ — MSSQL extended stored procedure execution",
		pattern: /\bEXEC(?:UTE)?\s{1,20}xp_/i
	},
	{
		id: "sql-tautology-string",
		description: "Classic string tautology: ' OR '1'='1 or \" OR \"1\"=\"1\"",
		pattern: /'\s{0,10}OR\s{0,10}'[^']{0,20}'\s*=\s*'[^']{0,20}/i
	},
	{
		id: "sql-tautology-numeric",
		description: "Numeric tautology: OR 1=1",
		pattern: /\bOR\s{1,10}1\s*=\s*1\b/i
	},
	{
		id: "sql-always-true-zero",
		description: "Numeric tautology: OR 0=0",
		pattern: /\bOR\s{1,10}0\s*=\s*0\b/i
	},
	{
		id: "sql-sleep-benchmark",
		description: "Time-based blind injection: SLEEP() or BENCHMARK()",
		pattern: /\b(?:SLEEP|BENCHMARK)\s*\(/i
	},
	{
		id: "sql-waitfor-delay",
		description: "MSSQL time-based blind injection: WAITFOR DELAY",
		pattern: /\bWAITFOR\s{1,20}DELAY\b/i
	},
	{
		id: "sql-char-function",
		description: "CHAR() function — used to obfuscate injected strings",
		pattern: /\bCHAR\s*\(\s*\d{1,3}/i
	},
	{
		id: "sql-information-schema",
		description: "INFORMATION_SCHEMA — reconnaissance query for table/column enumeration",
		pattern: /\bINFORMATION_SCHEMA\b/i
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/shell.js
/**
* SHELL context patterns.
*
* Detects shell injection vectors and path traversal patterns.
* Designed for use when a string will be passed to a shell command,
* used as a file path, or interpolated into OS-level operations.
*/
var SHELL_PATTERNS = [
	{
		id: "shell-path-traversal-unix",
		description: "Unix path traversal: ../  — climbing the directory tree",
		pattern: /\.\.\//
	},
	{
		id: "shell-path-traversal-windows",
		description: "Windows path traversal: ..\\ — climbing the directory tree",
		pattern: /\.\.\\/
	},
	{
		id: "shell-path-traversal-encoded",
		description: "URL-encoded path traversal: %2e%2e or %2f variants",
		pattern: /%2e%2e|%2f\.\.|\.\.%2f/i
	},
	{
		id: "shell-null-byte",
		description: "Null byte injection: \\x00 or %00 — truncates strings in C-backed functions",
		pattern: /\x00|%00/
	},
	{
		id: "shell-semicolon",
		description: "Semicolon command separator: cmd1; cmd2",
		pattern: /;/
	},
	{
		id: "shell-pipe",
		description: "Pipe operator: cmd1 | cmd2",
		pattern: /\|/
	},
	{
		id: "shell-and-operator",
		description: "AND operator: cmd1 && cmd2",
		pattern: /&&/
	},
	{
		id: "shell-or-operator",
		description: "OR operator: cmd1 || cmd2",
		pattern: /\|\|/
	},
	{
		id: "shell-backtick",
		description: "Backtick command substitution: `cmd`",
		pattern: /`/
	},
	{
		id: "shell-dollar-paren",
		description: "Dollar-paren command substitution: $(cmd)",
		pattern: /\$\(/
	},
	{
		id: "shell-dollar-brace",
		description: "Dollar-brace variable expansion: ${var} — can be abused for injection",
		pattern: /\$\{/
	},
	{
		id: "shell-redirect-out",
		description: "Output redirection: cmd > file or cmd >> file",
		pattern: />{1,2}/
	},
	{
		id: "shell-redirect-in",
		description: "Input redirection: cmd < file",
		pattern: /</
	},
	{
		id: "shell-newline-injection",
		description: "Newline injection: \\n or \\r — can inject new shell commands",
		pattern: /[\n\r]/
	},
	{
		id: "shell-glob-star",
		description: "Glob expansion: * or ? — can expand to unintended files",
		pattern: /[/\\][*?]/
	},
	{
		id: "shell-absolute-root",
		description: "Absolute root path injection: string starting with / or \\ (Windows UNC)",
		pattern: /^(?:\/|\\\\)/
	},
	{
		id: "shell-windows-drive",
		description: "Windows drive letter path injection: C:\\ or D:/",
		pattern: /^[a-zA-Z]:[/\\]/
	},
	{
		id: "shell-curl-wget",
		description: "curl/wget with URL or flags — can exfiltrate data or download payloads",
		pattern: /\b(?:curl|wget)\s+(?:https?:\/\/|ftp:\/\/|-)/i
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/redos.js
/**
* REDOS context patterns.
*
* Detects strings that, if used as regular expressions, could cause
* catastrophic backtracking (ReDoS — Regular Expression Denial of Service).
*
* These patterns detect the structural forms that lead to exponential or
* polynomial backtracking in NFA-based regex engines (V8, PCRE, Java, etc.).
*
* Use this context when user-supplied strings will be compiled into RegExp objects.
*/
var REDOS_PATTERNS = [
	{
		id: "redos-nested-quantifier-plus",
		description: "Nested + quantifier inside a group with outer quantifier: (a+)+, (.+b)*, etc.",
		pattern: /\([^)]*\+[^)]*\)[+*]/
	},
	{
		id: "redos-nested-quantifier-star",
		description: "Nested * quantifier: (a*)* or (a*)+ — catastrophic backtracking",
		pattern: /\([^)]*\*[^)]*\)[*+]/
	},
	{
		id: "redos-nested-groups",
		description: "Doubly nested quantified groups: ((a+)+) — guaranteed catastrophic",
		pattern: /\(\([^)]{0,40}\)[+*]\)[+*]/
	},
	{
		id: "redos-alternation-overlap",
		description: "Overlapping alternation under quantifier: (a|a)+ — ambiguous NFA paths",
		pattern: /\(([^|()]{1,20})\|(?:\1)(?:\|[^|()]{1,20}){0,5}\)[+*?]{1,2}/
	},
	{
		id: "redos-star-plus-concat",
		description: "(x*x)+ pattern — triggers super-linear backtracking",
		pattern: /\([^)]{0,10}\*[^)]{0,10}\)[+*]/
	},
	{
		id: "redos-dot-star-greedy",
		description: "(.*){n,} or (.+){n,} — repeated greedy dot quantifiers",
		pattern: /\(\.[*+]\)\{?\d/
	},
	{
		id: "redos-large-repetition",
		description: "Very large fixed or range repetition count {1000,} or {1000,n} — denial of service via backtracking",
		pattern: /\{\d{4,}(?:,\d*)?\}/
	},
	{
		id: "redos-catastrophic-alternation",
		description: "Long alternation with many similar branches — polynomial backtracking risk",
		pattern: /\([^)]{0,200}(?:\|[^|)]{0,50}){9,}\)/
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/nosql.js
var sep = "[\"'\\s]*:";
var NOSQL_PATTERNS = [
	{
		id: "nosql-where-operator",
		description: "$where — executes arbitrary JavaScript server-side in MongoDB",
		pattern: new RegExp(`\\$where${sep}`, "i")
	},
	{
		id: "nosql-ne-operator",
		description: "$ne — \"not equal\" operator used to bypass equality checks",
		pattern: new RegExp(`\\$ne${sep}`, "i")
	},
	{
		id: "nosql-gt-operator",
		description: "$gt — \"greater than\" used to bypass password/value checks",
		pattern: new RegExp(`\\$gte?${sep}`, "i")
	},
	{
		id: "nosql-lt-operator",
		description: "$lt / $lte — \"less than\" bypass variants",
		pattern: new RegExp(`\\$lte?${sep}`, "i")
	},
	{
		id: "nosql-regex-operator",
		description: "$regex — can be used to extract data character by character (blind injection)",
		pattern: new RegExp(`\\$regex${sep}`, "i")
	},
	{
		id: "nosql-or-operator",
		description: "$or — logical OR; used to create always-true conditions",
		pattern: new RegExp(`\\$or${sep}\\s*\\[`, "i")
	},
	{
		id: "nosql-and-operator",
		description: "$and — logical AND operator injection",
		pattern: new RegExp(`\\$and${sep}\\s*\\[`, "i")
	},
	{
		id: "nosql-nor-operator",
		description: "$nor — logical NOR operator injection",
		pattern: new RegExp(`\\$nor${sep}\\s*\\[`, "i")
	},
	{
		id: "nosql-exists-operator",
		description: "$exists — can enumerate fields to determine schema",
		pattern: new RegExp(`\\$exists${sep}`, "i")
	},
	{
		id: "nosql-in-operator",
		description: "$in — matches any value in a list; can enumerate values",
		pattern: new RegExp(`\\$in${sep}\\s*\\[`, "i")
	},
	{
		id: "nosql-expr-operator",
		description: "$expr — allows aggregation expressions in queries (MongoDB 3.6+)",
		pattern: new RegExp(`\\$expr${sep}`, "i")
	},
	{
		id: "nosql-function-operator",
		description: "$function — executes arbitrary JavaScript in MongoDB 4.4+",
		pattern: new RegExp(`\\$function${sep}`, "i")
	},
	{
		id: "nosql-accumulator-operator",
		description: "$accumulator — custom aggregation with arbitrary JS execution",
		pattern: new RegExp(`\\$accumulator${sep}`, "i")
	},
	{
		id: "nosql-proto-pollution",
		description: "__proto__ — prototype pollution via object key injection",
		pattern: /__proto__/
	},
	{
		id: "nosql-constructor-prototype",
		description: "constructor.prototype — alternative prototype pollution vector (dot notation or JSON key)",
		pattern: /constructor[\s"':.,{\[]*prototype/i
	},
	{
		id: "nosql-proto-bracket",
		description: "[\"__proto__\"] — bracket-notation prototype pollution",
		pattern: /\[["']__proto__["']\]/
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/log.js
/**
* LOG context patterns.
*
* Detects injection vectors that are dangerous when a string is written
* to a log file, passed to a logging framework, or interpolated into
* a log message that will be parsed or displayed.
*
* Attack categories:
*   1. CRLF injection — injects fake log lines by embedding newlines
*   2. Log4Shell (CVE-2021-44228) — ${jndi:...} triggers JNDI lookup in Log4j
*   3. SSTI in log templates — {{...}}, #{...} trigger template evaluation
*      if the log message is passed through a template engine
*   4. Null byte injection — truncates log entries in some implementations
*   5. ANSI escape injection — manipulates terminal output when logs are
*      tailed in a terminal (colour codes, cursor movement, etc.)
*
* Note: Newline characters (\n, \r) will produce false positives for
* multi-line legitimate values. Use this context only for single-line
* log field values (usernames, IDs, request parameters, etc.).
*/
var LOG_PATTERNS = [
	{
		id: "log-crlf-injection",
		description: "CRLF injection: literal \\r or \\n embeds fake log lines",
		pattern: /[\r\n]/
	},
	{
		id: "log-url-encoded-crlf",
		description: "URL-encoded CRLF: %0d, %0a, %0D, %0A — decoded by some log parsers",
		pattern: /%0[dDaA]/
	},
	{
		id: "log-unicode-newline",
		description: "Unicode newline variants: U+2028 (line separator), U+2029 (paragraph separator)",
		pattern: /[\u2028\u2029]/
	},
	{
		id: "log-log4shell-jndi",
		description: "Log4Shell: ${jndi:...} triggers remote code execution in Apache Log4j",
		pattern: /\$\{jndi\s*:/i
	},
	{
		id: "log-log4shell-obfuscated",
		description: "Obfuscated Log4Shell: ${::-j}... lookup-bypass prefix used to evade WAF detection",
		pattern: /\$\{::-/
	},
	{
		id: "log-log4j-lookup",
		description: "Log4j lookup syntax: ${env:...}, ${sys:...}, ${ctx:...} — data exfiltration",
		pattern: /\$\{(?:env|sys|ctx|main|map|sd|web|docker|k8s|spring)\s*:/i
	},
	{
		id: "log-ssti-double-brace",
		description: "SSTI double-brace: {{expression}} — Jinja2, Twig, Handlebars, etc.",
		pattern: /\{\{[\s\S]{0,80}\}\}/
	},
	{
		id: "log-ssti-hash-brace",
		description: "SSTI hash-brace: #{expression} — Thymeleaf, Velocity, Ruby ERB",
		pattern: /#\{[\s\S]{0,80}\}/
	},
	{
		id: "log-ssti-dollar-brace",
		description: "SSTI/EL injection: ${expression with operators or method calls} — JSP EL, Freemarker, SpEL",
		pattern: /\$\{[^}]*(?:\.|\(|\*|\+|\bclass\b|\bruntime\b|\bprocess\b|\bexec\b)[^}]{0,80}\}/i
	},
	{
		id: "log-ssti-percent-tag",
		description: "SSTI ERB/ASP tag: <%= expression %> — Ruby ERB, ASP",
		pattern: /<%=[\s\S]{0,80}%>/
	},
	{
		id: "log-null-byte",
		description: "Null byte: \\x00 or %00 — can truncate log entries in C-backed loggers",
		pattern: /\x00|%00/
	},
	{
		id: "log-ansi-escape",
		description: "ANSI escape sequence: ESC[ — can manipulate terminal output when logs are tailed",
		pattern: /\x1b\[/
	}
];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/contexts/sql-strict.js
/**
* SQL-STRICT context patterns.
*
* Extends the base 'SQL' context with three additional rules that are
* effective at detecting real injections but carry a higher false-positive
* risk on general free-text input.
*
* Use 'SQL-STRICT' when:
*   - The string is specifically a SQL fragment or database identifier
*   - You control the input domain (e.g. a dedicated SQL search field)
*   - You can tolerate occasional false positives in exchange for broader coverage
*
* Use 'SQL' (not STRICT) when:
*   - The field is general user text (names, descriptions, comments)
*   - False positives would block legitimate content (e.g. "see note -- above")
*
* Rules moved here from 'SQL' due to false-positive risk:
*
*   sql-line-comment   — "--" fires on "see note -- above", "value--", CSS var(--primary)
*   sql-stacked-query  — "; SELECT" fires on legitimate prose with semicolons + SQL words
*   sql-hex-encoding   — "0xDEAD" fires on hex values in technical docs and log output
*/
var SQL_STRICT_EXTRA = [
	{
		id: "sql-line-comment",
		description: "SQL line comment: -- followed by whitespace or end of string",
		pattern: /--(?:\s|$)/
	},
	{
		id: "sql-stacked-query",
		description: "Stacked queries: semicolon immediately followed by a SQL keyword",
		pattern: /;\s{0,10}(?:SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|EXEC)\b/i
	},
	{
		id: "sql-hex-encoding",
		description: "Hex-encoded string injection: 0x41414141 style (MySQL)",
		pattern: /\b0x[0-9a-f]{4,}/i
	}
];
var SQL_STRICT_PATTERNS = [...SQL_PATTERNS, ...SQL_STRICT_EXTRA];
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/is-unsafe@2.0.2/node_modules/is-unsafe/src/index.js
HTML_PATTERNS.label = "HTML";
XML_PATTERNS.label = "XML";
SVG_PATTERNS.label = "SVG";
SQL_PATTERNS.label = "SQL";
SQL_STRICT_PATTERNS.label = "SQL-STRICT";
SHELL_PATTERNS.label = "SHELL";
REDOS_PATTERNS.label = "REDOS";
NOSQL_PATTERNS.label = "NOSQL";
LOG_PATTERNS.label = "LOG";
Object.freeze({
	HTML: HTML_PATTERNS,
	XML: XML_PATTERNS,
	SVG: SVG_PATTERNS,
	SQL: SQL_PATTERNS,
	"SQL-STRICT": SQL_STRICT_PATTERNS,
	SHELL: SHELL_PATTERNS,
	REDOS: REDOS_PATTERNS,
	NOSQL: NOSQL_PATTERNS,
	LOG: LOG_PATTERNS
});
/**
* @typedef {{ id: string, description: string, pattern: RegExp }} Rule
*/
/**
* @typedef {Rule[]} PatternList
*/
/**
* @typedef {Object} MatchResult
* @property {string} context     - Label identifying which context matched ('HTML', 'CUSTOM', etc.)
* @property {string} id          - Rule identifier
* @property {string} description - Human-readable description of what was matched
* @property {RegExp} pattern     - The pattern that matched
*/
/**
* @param {unknown} value
*/
function assertString(value) {
	if (typeof value !== "string") throw new TypeError(`is-unsafe: first argument must be a string, got ${typeof value}`);
}
/**
* @param {unknown} context
*/
function assertContext(context) {
	if (context instanceof RegExp) return;
	if (Array.isArray(context)) {
		if (context.length === 0) throw new TypeError("is-unsafe: context must not be an empty array");
		if (Array.isArray(context[0])) {
			for (const list of context) if (!Array.isArray(list) || list.length === 0) throw new TypeError("is-unsafe: each context in the array must be a non-empty pattern array (PatternList)");
		}
		return;
	}
	throw new TypeError(`is-unsafe: second argument must be a PatternList (e.g. HTML), an array of PatternLists (e.g. [HTML, XML]), or a RegExp. Got: ${typeof context}`);
}
/**
* Normalise any valid context arg into an array of PatternLists.
*
* @param {Rule[]|Rule[][]|RegExp} context
* @returns {{ lists: Rule[][]|null, regex: RegExp|null }}
*/
function normalise(context) {
	if (context instanceof RegExp) return {
		lists: null,
		regex: context
	};
	if (Array.isArray(context[0])) return {
		lists: context,
		regex: null
	};
	return {
		lists: [context],
		regex: null
	};
}
/**
* Test value against a single PatternList. Returns the first MatchResult or null.
*
* @param {string} value
* @param {Rule[]} list
* @returns {MatchResult|null}
*/
function matchList(value, list) {
	const label = list.label ?? "CUSTOM";
	for (const rule of list) if (rule.pattern.test(value)) return {
		context: label,
		id: rule.id,
		description: rule.description,
		pattern: rule.pattern
	};
	return null;
}
/**
* Returns `true` if `value` is unsafe in the given context(s), `false` otherwise.
*
* @param {string} value - The string to test
* @param {PatternList | PatternList[] | RegExp} context
*   - A PatternList imported from is-unsafe (e.g. `HTML`, `XML`)
*   - An array of PatternLists — returns true if unsafe in **any** of them
*   - A custom RegExp — returns true if the pattern matches
* @returns {boolean}
*
* @example
* import { isUnsafe, HTML, SQL } from 'is-unsafe';
*
* isUnsafe('<script>alert(1)<\/script>', HTML)       // true
* isUnsafe('hello world', HTML)                     // false
* isUnsafe('value', [HTML, SQL])                    // false
* isUnsafe('value', /my-pattern/i)                  // false
*/
function isUnsafe(value, context) {
	assertString(value);
	assertContext(context);
	const { lists, regex } = normalise(context);
	if (regex) return regex.test(value);
	for (const list of lists) if (matchList(value, list) !== null) return true;
	return false;
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/OrderedObjParser.js
/**
* Extract raw attributes (without prefix) from prefixed attribute map
* @param {object} prefixedAttrs - Attributes with prefix from buildAttributesMap
* @param {object} options - Parser options containing attributeNamePrefix
* @returns {object} Raw attributes for matcher
*/
function extractRawAttributes(prefixedAttrs, options) {
	if (!prefixedAttrs) return {};
	const attrs = options.attributesGroupName ? prefixedAttrs[options.attributesGroupName] : prefixedAttrs;
	if (!attrs) return {};
	const rawAttrs = {};
	for (const key in attrs) if (key.startsWith(options.attributeNamePrefix)) {
		const rawName = key.substring(options.attributeNamePrefix.length);
		rawAttrs[rawName] = attrs[key];
	} else rawAttrs[key] = attrs[key];
	return rawAttrs;
}
/**
* Extract namespace from raw tag name
* @param {string} rawTagName - Tag name possibly with namespace (e.g., "soap:Envelope")
* @returns {string|undefined} Namespace or undefined
*/
function extractNamespace(rawTagName) {
	if (!rawTagName || typeof rawTagName !== "string") return void 0;
	const colonIndex = rawTagName.indexOf(":");
	if (colonIndex !== -1 && colonIndex > 0) {
		const ns = rawTagName.substring(0, colonIndex);
		if (ns !== "xmlns") return ns;
	}
}
var OrderedObjParser = class {
	constructor(options, externalEntities) {
		this.options = options;
		this.currentNode = null;
		this.tagsNodeStack = [];
		this.parseXml = parseXml;
		this.parseTextData = parseTextData;
		this.resolveNameSpace = resolveNameSpace;
		this.buildAttributesMap = buildAttributesMap;
		this.isItStopNode = isItStopNode;
		this.replaceEntitiesValue = replaceEntitiesValue;
		this.readStopNodeData = readStopNodeData;
		this.saveTextToParentTag = saveTextToParentTag;
		this.addChild = addChild;
		this.ignoreAttributesFn = getIgnoreAttributesFn(this.options.ignoreAttributes);
		this.entityExpansionCount = 0;
		this.currentExpandedLength = 0;
		this.doctypefound = false;
		let namedEntities = { ...XML };
		if (this.options.entityDecoder) this.entityDecoder = this.options.entityDecoder;
		else {
			if (typeof this.options.htmlEntities === "object") namedEntities = this.options.htmlEntities;
			else if (this.options.htmlEntities === true) namedEntities = {
				...COMMON_HTML,
				...CURRENCY
			};
			this.entityDecoder = new EntityDecoder({
				namedEntities: {
					...namedEntities,
					...externalEntities
				},
				numericAllowed: this.options.htmlEntities,
				limit: {
					maxTotalExpansions: this.options.processEntities.maxTotalExpansions,
					maxExpandedLength: this.options.processEntities.maxExpandedLength,
					applyLimitsTo: this.options.processEntities.appliesTo
				},
				onInputEntity: (name, value) => isUnsafe(value, [HTML_PATTERNS, XML_PATTERNS]) ? ENTITY_ACTION.BLOCK : ENTITY_ACTION.ALLOW
			});
		}
		this.matcher = new Matcher();
		this.readonlyMatcher = this.matcher.readOnly();
		this.isCurrentNodeStopNode = false;
		this.stopNodeExpressionsSet = new ExpressionSet();
		const stopNodesOpts = this.options.stopNodes;
		if (stopNodesOpts && stopNodesOpts.length > 0) {
			for (let i = 0; i < stopNodesOpts.length; i++) {
				const stopNodeExp = stopNodesOpts[i];
				if (typeof stopNodeExp === "string") this.stopNodeExpressionsSet.add(new Expression(stopNodeExp));
				else if (stopNodeExp instanceof Expression) this.stopNodeExpressionsSet.add(stopNodeExp);
			}
			this.stopNodeExpressionsSet.seal();
		}
	}
};
/**
* @param {string} val
* @param {string} tagName
* @param {string|Matcher} jPath - jPath string or Matcher instance based on options.jPath
* @param {boolean} dontTrim
* @param {boolean} hasAttributes
* @param {boolean} isLeafNode
* @param {boolean} escapeEntities
*/
function parseTextData(val, tagName, jPath, dontTrim, hasAttributes, isLeafNode, escapeEntities) {
	const options = this.options;
	if (val !== void 0) {
		if (options.trimValues && !dontTrim) val = val.trim();
		if (val.length > 0) {
			if (!escapeEntities) val = this.replaceEntitiesValue(val, tagName, jPath);
			const jPathOrMatcher = options.jPath ? jPath.toString() : jPath;
			const newval = options.tagValueProcessor(tagName, val, jPathOrMatcher, hasAttributes, isLeafNode);
			if (newval === null || newval === void 0) return val;
			else if (typeof newval !== typeof val || newval !== val) return newval;
			else if (options.trimValues) return parseValue(val, options.parseTagValue, options.numberParseOptions);
			else if (val.trim() === val) return parseValue(val, options.parseTagValue, options.numberParseOptions);
			else return val;
		}
	}
}
function resolveNameSpace(tagname) {
	if (this.options.removeNSPrefix) {
		const tags = tagname.split(":");
		const prefix = tagname.charAt(0) === "/" ? "/" : "";
		if (tags[0] === "xmlns") return "";
		if (tags.length === 2) tagname = prefix + tags[1];
	}
	return tagname;
}
var attrsRegx = /* @__PURE__ */ new RegExp("([^\\s=]+)\\s*(=\\s*(['\"])([\\s\\S]*?)\\3)?", "gm");
function buildAttributesMap(attrStr, jPath, tagName, force = false) {
	const options = this.options;
	if (force === true || options.ignoreAttributes !== true && typeof attrStr === "string") {
		const matches = getAllMatches(attrStr, attrsRegx);
		const len = matches.length;
		const attrs = {};
		const processedVals = new Array(len);
		let hasRawAttrs = false;
		const rawAttrsForMatcher = {};
		for (let i = 0; i < len; i++) {
			const attrName = this.resolveNameSpace(matches[i][1]);
			const oldVal = matches[i][4];
			if (attrName.length && oldVal !== void 0) {
				let val = oldVal;
				if (options.trimValues) val = val.trim();
				val = this.replaceEntitiesValue(val, tagName, this.readonlyMatcher);
				processedVals[i] = val;
				rawAttrsForMatcher[attrName] = val;
				hasRawAttrs = true;
			}
		}
		if (hasRawAttrs && typeof jPath === "object" && jPath.updateCurrent) jPath.updateCurrent(rawAttrsForMatcher);
		const jPathStr = options.jPath ? jPath.toString() : this.readonlyMatcher;
		let hasAttrs = false;
		for (let i = 0; i < len; i++) {
			const attrName = this.resolveNameSpace(matches[i][1]);
			if (this.ignoreAttributesFn(attrName, jPathStr)) continue;
			let aName = options.attributeNamePrefix + attrName;
			if (attrName.length) {
				if (options.transformAttributeName) aName = options.transformAttributeName(aName);
				aName = sanitizeName(aName, options);
				if (matches[i][4] !== void 0) {
					const oldVal = processedVals[i];
					const newVal = options.attributeValueProcessor(attrName, oldVal, jPathStr);
					if (newVal === null || newVal === void 0) attrs[aName] = oldVal;
					else if (typeof newVal !== typeof oldVal || newVal !== oldVal) attrs[aName] = newVal;
					else attrs[aName] = parseValue(oldVal, options.parseAttributeValue, options.numberParseOptions);
					hasAttrs = true;
				} else if (options.allowBooleanAttributes) {
					attrs[aName] = true;
					hasAttrs = true;
				}
			}
		}
		if (!hasAttrs) return;
		if (options.attributesGroupName && !options.preserveOrder) {
			const attrCollection = {};
			attrCollection[options.attributesGroupName] = attrs;
			return attrCollection;
		}
		return attrs;
	}
}
var parseXml = function(xmlData) {
	xmlData = xmlData.replace(/\r\n?/g, "\n");
	const xmlObj = new XmlNode("!xml");
	let currentNode = xmlObj;
	let textData = "";
	this.matcher.reset();
	this.entityDecoder.reset();
	this.entityExpansionCount = 0;
	this.currentExpandedLength = 0;
	this.doctypefound = false;
	const options = this.options;
	const docTypeReader = new DocTypeReader(options.processEntities);
	const xmlLen = xmlData.length;
	for (let i = 0; i < xmlLen; i++) if (xmlData[i] === "<") {
		const c1 = xmlData.charCodeAt(i + 1);
		if (c1 === 47) {
			const closeIndex = findClosingIndex(xmlData, ">", i, "Closing Tag is not closed.");
			let tagName = xmlData.substring(i + 2, closeIndex).trim();
			if (options.removeNSPrefix) {
				const colonIndex = tagName.indexOf(":");
				if (colonIndex !== -1) tagName = tagName.substr(colonIndex + 1);
			}
			tagName = transformTagName(options.transformTagName, tagName, "", options).tagName;
			if (currentNode) textData = this.saveTextToParentTag(textData, currentNode, this.readonlyMatcher);
			const lastTagName = this.matcher.getCurrentTag();
			if (tagName && options.unpairedTagsSet.has(tagName)) throw new Error(`Unpaired tag can not be used as closing tag: </${tagName}>`);
			if (lastTagName && options.unpairedTagsSet.has(lastTagName)) {
				this.matcher.pop();
				this.tagsNodeStack.pop();
			}
			this.matcher.pop();
			this.isCurrentNodeStopNode = false;
			currentNode = this.tagsNodeStack.pop() || xmlObj;
			if (options.captureMetaData && currentNode) currentNode.addEndIndex(closeIndex + 1);
			textData = "";
			i = closeIndex;
		} else if (c1 === 63) {
			let tagData = readTagExp(xmlData, i, false, "?>");
			if (!tagData) throw new Error("Pi Tag is not closed.");
			textData = this.saveTextToParentTag(textData, currentNode, this.readonlyMatcher);
			const attsMap = this.buildAttributesMap(tagData.tagExp, this.matcher, tagData.tagName, true);
			if (attsMap) {
				const ver = attsMap[this.options.attributeNamePrefix + "version"];
				this.entityDecoder.setXmlVersion(Number(ver) || 1);
				docTypeReader.setXmlVersion(Number(ver) || 1);
			}
			if (options.ignoreDeclaration && tagData.tagName === "?xml" || options.ignorePiTags) {} else {
				const childNode = new XmlNode(tagData.tagName);
				childNode.add(options.textNodeName, "");
				if (tagData.tagName !== tagData.tagExp && tagData.attrExpPresent && options.ignoreAttributes !== true) childNode[":@"] = attsMap;
				this.addChild(currentNode, childNode, this.readonlyMatcher, i);
				if (options.captureMetaData) currentNode.addEndIndex(tagData.closeIndex + 2);
			}
			i = tagData.closeIndex + 1;
		} else if (c1 === 33 && xmlData.charCodeAt(i + 2) === 45 && xmlData.charCodeAt(i + 3) === 45) {
			const endIndex = findClosingIndex(xmlData, "-->", i + 4, "Comment is not closed.");
			if (options.commentPropName) {
				const comment = xmlData.substring(i + 4, endIndex - 2);
				textData = this.saveTextToParentTag(textData, currentNode, this.readonlyMatcher);
				currentNode.add(options.commentPropName, [{ [options.textNodeName]: comment }]);
			}
			i = endIndex;
		} else if (c1 === 33 && xmlData.charCodeAt(i + 2) === 68) {
			if (this.doctypefound) throw new Error("Multiple DOCTYPE declarations found.");
			this.doctypefound = true;
			const result = docTypeReader.readDocType(xmlData, i);
			this.entityDecoder.addInputEntities(result.entities);
			i = result.i;
		} else if (c1 === 33 && xmlData.charCodeAt(i + 2) === 91) {
			const closeIndex = findClosingIndex(xmlData, "]]>", i, "CDATA is not closed.") - 2;
			const tagExp = xmlData.substring(i + 9, closeIndex);
			textData = this.saveTextToParentTag(textData, currentNode, this.readonlyMatcher);
			let val = this.parseTextData(tagExp, currentNode.tagname, this.readonlyMatcher, true, false, true, true);
			if (val == void 0) val = "";
			if (options.cdataPropName) currentNode.add(options.cdataPropName, [{ [options.textNodeName]: tagExp }]);
			else currentNode.add(options.textNodeName, val);
			i = closeIndex + 2;
		} else {
			let result = readTagExp(xmlData, i, options.removeNSPrefix);
			if (!result) {
				const context = xmlData.substring(Math.max(0, i - 50), Math.min(xmlLen, i + 50));
				throw new Error(`readTagExp returned undefined at position ${i}. Context: "${context}"`);
			}
			let tagName = result.tagName;
			const rawTagName = result.rawTagName;
			let tagExp = result.tagExp;
			let attrExpPresent = result.attrExpPresent;
			let closeIndex = result.closeIndex;
			({tagName, tagExp} = transformTagName(options.transformTagName, tagName, tagExp, options));
			if (options.strictReservedNames && (tagName === options.commentPropName || tagName === options.cdataPropName || tagName === options.textNodeName || tagName === options.attributesGroupName)) throw new Error(`Invalid tag name: ${tagName}`);
			if (currentNode && textData) {
				if (currentNode.tagname !== "!xml") textData = this.saveTextToParentTag(textData, currentNode, this.readonlyMatcher, false);
			}
			const lastTag = currentNode;
			if (lastTag && options.unpairedTagsSet.has(lastTag.tagname)) {
				currentNode = this.tagsNodeStack.pop();
				this.matcher.pop();
			}
			let isSelfClosing = false;
			if (tagExp.length > 0 && tagExp.lastIndexOf("/") === tagExp.length - 1) {
				isSelfClosing = true;
				if (tagName[tagName.length - 1] === "/") {
					tagName = tagName.substr(0, tagName.length - 1);
					tagExp = tagName;
				} else tagExp = tagExp.substr(0, tagExp.length - 1);
				attrExpPresent = tagName !== tagExp;
			}
			let prefixedAttrs = null;
			let namespace = void 0;
			namespace = extractNamespace(rawTagName);
			if (tagName !== xmlObj.tagname) this.matcher.push(tagName, {}, namespace);
			if (tagName !== tagExp && attrExpPresent) {
				prefixedAttrs = this.buildAttributesMap(tagExp, this.matcher, tagName);
				if (prefixedAttrs) extractRawAttributes(prefixedAttrs, options);
			}
			if (tagName !== xmlObj.tagname) this.isCurrentNodeStopNode = this.isItStopNode();
			const startIndex = i;
			if (this.isCurrentNodeStopNode) {
				let tagContent = "";
				if (isSelfClosing) i = result.closeIndex;
				else if (options.unpairedTagsSet.has(tagName)) i = result.closeIndex;
				else {
					const result = this.readStopNodeData(xmlData, rawTagName, closeIndex + 1);
					if (!result) throw new Error(`Unexpected end of ${rawTagName}`);
					i = result.i;
					tagContent = result.tagContent;
				}
				const childNode = new XmlNode(tagName);
				if (prefixedAttrs) childNode[":@"] = prefixedAttrs;
				childNode.add(options.textNodeName, tagContent);
				this.matcher.pop();
				this.isCurrentNodeStopNode = false;
				this.addChild(currentNode, childNode, this.readonlyMatcher, startIndex);
				if (options.captureMetaData) currentNode.addEndIndex(i + 1);
			} else {
				if (isSelfClosing) {
					({tagName, tagExp} = transformTagName(options.transformTagName, tagName, tagExp, options));
					const childNode = new XmlNode(tagName);
					if (prefixedAttrs) childNode[":@"] = prefixedAttrs;
					this.addChild(currentNode, childNode, this.readonlyMatcher, startIndex);
					if (options.captureMetaData) currentNode.addEndIndex(closeIndex + 1);
					this.matcher.pop();
					this.isCurrentNodeStopNode = false;
				} else if (options.unpairedTagsSet.has(tagName)) {
					const childNode = new XmlNode(tagName);
					if (prefixedAttrs) childNode[":@"] = prefixedAttrs;
					this.addChild(currentNode, childNode, this.readonlyMatcher, startIndex);
					if (options.captureMetaData) currentNode.addEndIndex(result.closeIndex + 1);
					this.matcher.pop();
					this.isCurrentNodeStopNode = false;
					i = result.closeIndex;
					continue;
				} else {
					const childNode = new XmlNode(tagName);
					if (this.tagsNodeStack.length > options.maxNestedTags) throw new Error("Maximum nested tags exceeded");
					this.tagsNodeStack.push(currentNode);
					if (prefixedAttrs) childNode[":@"] = prefixedAttrs;
					this.addChild(currentNode, childNode, this.readonlyMatcher, startIndex);
					currentNode = childNode;
				}
				textData = "";
				i = closeIndex;
			}
		}
	} else textData += xmlData[i];
	return xmlObj.child;
};
function addChild(currentNode, childNode, matcher, startIndex) {
	if (!this.options.captureMetaData) startIndex = void 0;
	const jPathOrMatcher = this.options.jPath ? matcher.toString() : matcher;
	const result = this.options.updateTag(childNode.tagname, jPathOrMatcher, childNode[":@"]);
	if (result === false) {} else if (typeof result === "string") {
		childNode.tagname = result;
		currentNode.addChild(childNode, startIndex);
	} else currentNode.addChild(childNode, startIndex);
}
/**
* @param {object} val - Entity object with regex and val properties
* @param {string} tagName - Tag name
* @param {string|Matcher} jPath - jPath string or Matcher instance based on options.jPath
*/
function replaceEntitiesValue(val, tagName, jPath) {
	const entityConfig = this.options.processEntities;
	if (!entityConfig || !entityConfig.enabled) return val;
	if (entityConfig.allowedTags) {
		const jPathOrMatcher = this.options.jPath ? jPath.toString() : jPath;
		if (!(Array.isArray(entityConfig.allowedTags) ? entityConfig.allowedTags.includes(tagName) : entityConfig.allowedTags(tagName, jPathOrMatcher))) return val;
	}
	if (entityConfig.tagFilter) {
		const jPathOrMatcher = this.options.jPath ? jPath.toString() : jPath;
		if (!entityConfig.tagFilter(tagName, jPathOrMatcher)) return val;
	}
	return this.entityDecoder.decode(val);
}
function saveTextToParentTag(textData, parentNode, matcher, isLeafNode) {
	if (textData) {
		if (isLeafNode === void 0) isLeafNode = parentNode.child.length === 0;
		textData = this.parseTextData(textData, parentNode.tagname, matcher, false, parentNode[":@"] ? Object.keys(parentNode[":@"]).length !== 0 : false, isLeafNode);
		if (textData !== void 0 && textData !== "") parentNode.add(this.options.textNodeName, textData);
		textData = "";
	}
	return textData;
}
/**
* @param {Array<Expression>} stopNodeExpressions - Array of compiled Expression objects
* @param {Matcher} matcher - Current path matcher
*/
function isItStopNode() {
	if (this.stopNodeExpressionsSet.size === 0) return false;
	return this.matcher.matchesAny(this.stopNodeExpressionsSet);
}
/**
* Returns the tag Expression and where it is ending handling single-double quotes situation
* @param {string} xmlData 
* @param {number} i starting index
* @returns 
*/
function tagExpWithClosingIndex(xmlData, i, closingChar = ">") {
	let attrBoundary = 0;
	const len = xmlData.length;
	const closeCode0 = closingChar.charCodeAt(0);
	const closeCode1 = closingChar.length > 1 ? closingChar.charCodeAt(1) : -1;
	let result = "";
	let segmentStart = i;
	for (let index = i; index < len; index++) {
		const code = xmlData.charCodeAt(index);
		if (attrBoundary) {
			if (code === attrBoundary) attrBoundary = 0;
		} else if (code === 34 || code === 39) attrBoundary = code;
		else if (code === closeCode0) if (closeCode1 !== -1) {
			if (xmlData.charCodeAt(index + 1) === closeCode1) {
				result += xmlData.substring(segmentStart, index);
				return {
					data: result,
					index
				};
			}
		} else {
			result += xmlData.substring(segmentStart, index);
			return {
				data: result,
				index
			};
		}
		else if (code === 9 && !attrBoundary) {
			result += xmlData.substring(segmentStart, index) + " ";
			segmentStart = index + 1;
		}
	}
}
function findClosingIndex(xmlData, str, i, errMsg) {
	const closingIndex = xmlData.indexOf(str, i);
	if (closingIndex === -1) throw new Error(errMsg);
	else return closingIndex + str.length - 1;
}
function findClosingChar(xmlData, char, i, errMsg) {
	const closingIndex = xmlData.indexOf(char, i);
	if (closingIndex === -1) throw new Error(errMsg);
	return closingIndex;
}
function readTagExp(xmlData, i, removeNSPrefix, closingChar = ">") {
	const result = tagExpWithClosingIndex(xmlData, i + 1, closingChar);
	if (!result) return;
	let tagExp = result.data;
	const closeIndex = result.index;
	const separatorIndex = tagExp.search(/\s/);
	let tagName = tagExp;
	let attrExpPresent = true;
	if (separatorIndex !== -1) {
		tagName = tagExp.substring(0, separatorIndex);
		tagExp = tagExp.substring(separatorIndex + 1).trimStart();
	}
	const rawTagName = tagName;
	if (removeNSPrefix) {
		const colonIndex = tagName.indexOf(":");
		if (colonIndex !== -1) {
			tagName = tagName.substr(colonIndex + 1);
			attrExpPresent = tagName !== result.data.substr(colonIndex + 1);
		}
	}
	return {
		tagName,
		tagExp,
		closeIndex,
		attrExpPresent,
		rawTagName
	};
}
/**
* find paired tag for a stop node
* @param {string} xmlData 
* @param {string} tagName 
* @param {number} i 
*/
function readStopNodeData(xmlData, tagName, i) {
	const startIndex = i;
	let openTagCount = 1;
	const xmllen = xmlData.length;
	for (; i < xmllen; i++) if (xmlData[i] === "<") {
		const c1 = xmlData.charCodeAt(i + 1);
		if (c1 === 47) {
			const closeIndex = findClosingChar(xmlData, ">", i, `${tagName} is not closed`);
			if (xmlData.substring(i + 2, closeIndex).trim() === tagName) {
				openTagCount--;
				if (openTagCount === 0) return {
					tagContent: xmlData.substring(startIndex, i),
					i: closeIndex
				};
			}
			i = closeIndex;
		} else if (c1 === 63) i = findClosingIndex(xmlData, "?>", i + 1, "StopNode is not closed.");
		else if (c1 === 33 && xmlData.charCodeAt(i + 2) === 45 && xmlData.charCodeAt(i + 3) === 45) i = findClosingIndex(xmlData, "-->", i + 3, "StopNode is not closed.");
		else if (c1 === 33 && xmlData.charCodeAt(i + 2) === 91) i = findClosingIndex(xmlData, "]]>", i, "StopNode is not closed.") - 2;
		else {
			const tagData = readTagExp(xmlData, i, false);
			if (tagData) {
				if ((tagData && tagData.tagName) === tagName && tagData.tagExp[tagData.tagExp.length - 1] !== "/") openTagCount++;
				i = tagData.closeIndex;
			}
		}
	}
}
function parseValue(val, shouldParse, options) {
	if (shouldParse && typeof val === "string") {
		const newval = val.trim();
		if (newval === "true") return true;
		else if (newval === "false") return false;
		else return toNumber(val, options);
	} else if (isExist(val)) return val;
	else return "";
}
function transformTagName(fn, tagName, tagExp, options) {
	if (fn) {
		const newTagName = fn(tagName);
		if (tagExp === tagName) tagExp = newTagName;
		tagName = newTagName;
	}
	tagName = sanitizeName(tagName, options);
	return {
		tagName,
		tagExp
	};
}
function sanitizeName(name, options) {
	if (criticalProperties.includes(name)) throw new Error(`[SECURITY] Invalid name: "${name}" is a reserved JavaScript keyword that could cause prototype pollution`);
	else if (DANGEROUS_PROPERTY_NAMES.includes(name)) return options.onDangerousProperty(name);
	return name;
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/node2json.js
var METADATA_SYMBOL = XmlNode.getMetaDataSymbol();
/**
* Helper function to strip attribute prefix from attribute map
* @param {object} attrs - Attributes with prefix (e.g., {"@_class": "code"})
* @param {string} prefix - Attribute prefix to remove (e.g., "@_")
* @returns {object} Attributes without prefix (e.g., {"class": "code"})
*/
function stripAttributePrefix(attrs, prefix) {
	if (!attrs || typeof attrs !== "object") return {};
	if (!prefix) return attrs;
	const rawAttrs = {};
	for (const key in attrs) if (key.startsWith(prefix)) {
		const rawName = key.substring(prefix.length);
		rawAttrs[rawName] = attrs[key];
	} else rawAttrs[key] = attrs[key];
	return rawAttrs;
}
/**
* 
* @param {array} node 
* @param {any} options 
* @param {Matcher} matcher - Path matcher instance
* @returns 
*/
function prettify(node, options, matcher, readonlyMatcher) {
	return compress(node, options, matcher, readonlyMatcher);
}
/**
* @param {array} arr 
* @param {object} options 
* @param {Matcher} matcher - Path matcher instance
* @returns object
*/
function compress(arr, options, matcher, readonlyMatcher) {
	let text;
	const compressedObj = {};
	for (let i = 0; i < arr.length; i++) {
		const tagObj = arr[i];
		const property = propName(tagObj);
		if (property !== void 0 && property !== options.textNodeName) {
			const rawAttrs = stripAttributePrefix(tagObj[":@"] || {}, options.attributeNamePrefix);
			matcher.push(property, rawAttrs);
		}
		if (property === options.textNodeName) if (text === void 0) text = tagObj[property];
		else text += "" + tagObj[property];
		else if (property === void 0) continue;
		else if (tagObj[property]) {
			let val = compress(tagObj[property], options, matcher, readonlyMatcher);
			const isLeaf = isLeafTag(val, options);
			if (Object.keys(val).length === 0 && options.alwaysCreateTextNode) val[options.textNodeName] = "";
			if (tagObj[":@"]) assignAttributes(val, tagObj[":@"], readonlyMatcher, options);
			else if (Object.keys(val).length === 1 && val[options.textNodeName] !== void 0 && !options.alwaysCreateTextNode) val = val[options.textNodeName];
			else if (Object.keys(val).length === 0) if (options.alwaysCreateTextNode) val[options.textNodeName] = "";
			else val = "";
			if (tagObj[METADATA_SYMBOL] !== void 0 && typeof val === "object" && val !== null) val[METADATA_SYMBOL] = tagObj[METADATA_SYMBOL];
			if (compressedObj[property] !== void 0 && Object.prototype.hasOwnProperty.call(compressedObj, property)) {
				if (!Array.isArray(compressedObj[property])) compressedObj[property] = [compressedObj[property]];
				compressedObj[property].push(val);
			} else {
				const jPathOrMatcher = options.jPath ? readonlyMatcher.toString() : readonlyMatcher;
				if (options.isArray(property, jPathOrMatcher, isLeaf)) compressedObj[property] = [val];
				else compressedObj[property] = val;
			}
			if (property !== void 0 && property !== options.textNodeName) matcher.pop();
		}
	}
	if (typeof text === "string") {
		if (text.length > 0) compressedObj[options.textNodeName] = text;
	} else if (text !== void 0) compressedObj[options.textNodeName] = text;
	return compressedObj;
}
function propName(obj) {
	const keys = Object.keys(obj);
	for (let i = 0; i < keys.length; i++) {
		const key = keys[i];
		if (key !== ":@") return key;
	}
}
function assignAttributes(obj, attrMap, readonlyMatcher, options) {
	if (attrMap) {
		const keys = Object.keys(attrMap);
		const len = keys.length;
		for (let i = 0; i < len; i++) {
			const atrrName = keys[i];
			const rawAttrName = atrrName.startsWith(options.attributeNamePrefix) ? atrrName.substring(options.attributeNamePrefix.length) : atrrName;
			const jPathOrMatcher = options.jPath ? readonlyMatcher.toString() + "." + rawAttrName : readonlyMatcher;
			if (options.isArray(atrrName, jPathOrMatcher, true, true)) obj[atrrName] = [attrMap[atrrName]];
			else obj[atrrName] = attrMap[atrrName];
		}
	}
}
function isLeafTag(obj, options) {
	const { textNodeName } = options;
	const propCount = Object.keys(obj).length;
	if (propCount === 0) return true;
	if (propCount === 1 && (obj[textNodeName] || typeof obj[textNodeName] === "boolean" || obj[textNodeName] === 0)) return true;
	return false;
}
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/xmlparser/XMLParser.js
var XMLParser = class {
	constructor(options) {
		this.externalEntities = {};
		this.options = buildOptions(options);
	}
	/**
	* Parse XML dats to JS object 
	* @param {string|Uint8Array} xmlData 
	* @param {boolean|Object} validationOption 
	*/
	parse(xmlData, validationOption) {
		if (typeof xmlData !== "string" && xmlData.toString) if (xmlData instanceof Uint8Array && !(typeof Buffer !== "undefined" && Buffer.isBuffer(xmlData))) xmlData = new TextDecoder("utf-8", { ignoreBOM: true }).decode(xmlData);
		else xmlData = xmlData.toString();
		else if (typeof xmlData !== "string") throw new Error("XML data is accepted in String or Bytes[] form.");
		if (validationOption) {
			if (validationOption === true) validationOption = {};
			const result = validate(xmlData, validationOption);
			if (result !== true) throw Error(`${result.err.msg}:${result.err.line}:${result.err.col}`);
		}
		const orderedObjParser = new OrderedObjParser(this.options, this.externalEntities);
		const orderedResult = orderedObjParser.parseXml(xmlData);
		if (this.options.preserveOrder || orderedResult === void 0) return orderedResult;
		else return prettify(orderedResult, this.options, orderedObjParser.matcher, orderedObjParser.readonlyMatcher);
	}
	/**
	* Add Entity which is not by default supported by this library
	* @param {string} key 
	* @param {string} value 
	*/
	addEntity(key, value) {
		if (value.indexOf("&") !== -1) throw new Error("Entity value can't have '&'");
		else if (key.indexOf("&") !== -1 || key.indexOf(";") !== -1) throw new Error("An entity must be set without '&' and ';'. Eg. use '#xD' for '&#xD;'");
		else if (value === "&") throw new Error("An entity with value '&' is not permitted");
		else this.externalEntities[key] = value;
	}
	/**
	* Returns a Symbol that can be used to access the metadata
	* property on a node.
	* 
	* If Symbol is not available in the environment, an ordinary property is used
	* and the name of the property is here returned.
	* 
	* The XMLMetaData property is only present when `captureMetaData`
	* is true in the options.
	*/
	static getMetaDataSymbol() {
		return XmlNode.getMetaDataSymbol();
	}
};
//#endregion
//#region ../../../sites/uijeongbu-did/node_modules/.pnpm/fast-xml-parser@5.11.2/node_modules/fast-xml-parser/src/fxp.js
var XMLValidator = { validate };
//#endregion
//#region lib/births.ts
var emptyBirths = () => ({
	months: [],
	savedAt: null,
	checkedAt: null,
	error: null,
	pending: false
});
//#endregion
//#region lib/birth-store.ts
function db$1() {
	if (!env.DB) throw Error("출생등록 DB 연결 확인 필요");
	return env.DB;
}
var SNAPSHOT = "births:snapshot:v1";
async function read() {
	const r = await db$1().prepare("SELECT payload FROM population_jobs WHERE id=?").bind(SNAPSHOT).first();
	return r ? JSON.parse(r.payload) : emptyBirths();
}
async function collectBirths(force = false) {
	const now = Date.now();
	if (!(await db$1().prepare("INSERT INTO collection_state(id,next_due,lease_until,last_started) VALUES('births',0,?,?) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND (collection_state.next_due<=? OR ?=1)").bind(now + 12e4, now, now, now, force ? 1 : 0).run()).meta.changes) return;
	const old = await read();
	const data = {
		...old,
		pending: false,
		checkedAt: new Date(now).toISOString(),
		error: null
	};
	try {
		let key = env.BIRTH_API_KEY;
		if (!key) throw Error("출생등록 인증키 설정 확인 필요");
		try {
			key = decodeURIComponent(key);
		} catch {}
		const latest = recentMonths()[0], first = `${Number(latest.slice(0, 4)) - 1}01`;
		const wanted = [];
		for (let m = latest; m >= first; m = monthOffset(m, -1)) wanted.push(m);
		const targets = wanted.filter((m) => !old.months.some((x) => x.month === m)).slice(0, 3);
		const parsed = (await Promise.all(targets.map(async (month) => {
			const u = new URL("https://apis.data.go.kr/1741000/admmSexdBrthReg/selectAdmmSexdBrthReg");
			Object.entries({
				serviceKey: key,
				type: "json",
				numOfRows: "100",
				pageNo: "1",
				lv: "3",
				admmCd: "4115000000",
				srchFrYm: month,
				srchToYm: month
			}).forEach(([k, v]) => u.searchParams.set(k, v));
			const r = await fetch(u, { signal: AbortSignal.timeout(2e4) });
			if (!r.ok) throw Error(`출생등록 제공기관 응답 오류 (${r.status}) · 기존 자료 유지`);
			const text = await r.text();
			let json;
			try {
				json = JSON.parse(text);
			} catch {
				try {
					json = new XMLParser().parse(text);
				} catch {
					throw Error("출생등록 응답 형식 확인 필요");
				}
			}
			const result = rowsAndCount(json);
			if (!result.rows.length || result.total !== null && result.total !== result.rows.length) throw Error("출생등록 월별 전체 자료 확인 필요");
			const rows = result.rows, example = rows[0];
			const map = fieldMap({
				params: [],
				fields: {}
			}, example);
			for (const [target, pattern] of Object.entries({
				population: /^(tot|total).*(brth|birth|reg).*cnt$/i,
				male: /^male.*(brth|birth|reg).*cnt$/i,
				female: /^(feml|female).*(brth|birth|reg).*cnt$/i
			})) if (!map[target]) {
				const k = Object.keys(example).find((k) => pattern.test(k));
				if (k) map[target] = k;
			}
			if (!map.population || !map.male || !map.female) throw Error(`출생등록 응답 항목 확인 필요: ${Object.keys(example).join(", ").slice(0, 400)}`);
			const parsed = normalizePopulation(rows.map((r) => ({
				...r,
				__households: 0
			})), {
				...map,
				households: "__households"
			});
			if (parsed.length !== 1 || parsed[0].month !== month || parsed.some((m) => !m.complete || m.notes.some((n) => n.includes("차이")))) throw Error("출생등록 15개 동 합계 검증 필요 · 기존 자료 유지");
			return parsed;
		}))).flat();
		const merged = new Map(old.months.map((m) => [m.month, m]));
		parsed.forEach((m) => merged.set(m.month, m));
		data.months = [...merged.values()].sort((a, b) => b.month.localeCompare(a.month)).slice(0, 36);
		if (parsed.length) data.savedAt = (/* @__PURE__ */ new Date()).toISOString();
		data.pending = wanted.some((m) => !merged.has(m));
	} catch (e) {
		data.error = e instanceof Error && e.name !== "TimeoutError" ? e.message : "출생등록 제공기관 연결 지연 · 기존 자료 유지";
	}
	await db$1().prepare("INSERT INTO population_jobs(id,payload,saved_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(SNAPSHOT, JSON.stringify(data), Date.now()).run();
	await db$1().prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id='births' AND last_started=?").bind(Date.now() + (data.error ? 15 * 6e4 : data.pending ? 1e3 : DAY$2), Date.now(), data.error, now).run();
}
async function loadBirths() {
	const [data, state] = await Promise.all([read(), db$1().prepare("SELECT lease_until,next_due FROM collection_state WHERE id='births'").first()]);
	const pending = Boolean(data.pending && !data.error) || Boolean(state && state.lease_until > Date.now());
	return {
		...data,
		pending
	};
}
//#endregion
//#region lib/commerce.ts
var COMMERCE_REGIONS = POP_REGIONS;
var CONVENIENCE = [
	{
		id: "convenience",
		name: "편의점",
		pattern: "편의점"
	},
	{
		id: "grocery",
		name: "마트·식료품",
		pattern: "슈퍼마켓|마트|식료품|청과|채소|과일|정육|육류|수산물"
	},
	{
		id: "laundry",
		name: "세탁소",
		pattern: "세탁"
	},
	{
		id: "beauty",
		name: "미용·관리",
		pattern: "미용|이용업|이용원|이발|피부 관리|손.*발톱.*관리|네일|메이크업"
	},
	{
		id: "pharmacy",
		name: "약국",
		pattern: "약국"
	},
	{
		id: "cafe",
		name: "카페",
		pattern: "카페|커피|다방"
	},
	{
		id: "restaurant",
		name: "음식점(카페 제외)",
		pattern: ""
	},
	{
		id: "education",
		name: "교육·학원",
		pattern: ""
	},
	{
		id: "health",
		name: "보건의료",
		pattern: ""
	},
	{
		id: "vehicle",
		name: "자동차정비·세차",
		pattern: "자동차.*(정비|수리|세차)|정비소|세차장"
	}
];
var convenienceKind = (s) => CONVENIENCE.find((c) => {
	if (c.id === "restaurant") return isFood(s) && !/카페|커피|다방/.test(s.small);
	if (c.id === "education") return s.largeCode === "P1" || /교육/.test(s.large);
	if (c.id === "health") return s.largeCode === "Q1" || /보건의료/.test(s.large);
	return new RegExp(c.pattern).test(s.small) && (c.id !== "cafe" || isFood(s)) && (c.id !== "grocery" || s.largeCode === "G2" || /소매/.test(s.large) || /소매|마트|슈퍼마켓/.test(s.small)) && (!["beauty", "laundry"].includes(c.id) || !/소매|도매|제조/.test(s.small));
})?.id ?? null;
var isFood = (s) => /음식|식당/.test(s.large) || s.largeCode === "I2" || s.largeCode === "Q";
function text$1(row, key) {
	return String(row[key] ?? "").trim();
}
function coordinate(v, min, max) {
	if (v === null || v === void 0 || v === "") return null;
	const n = Number(v);
	return Number.isFinite(n) && n >= min && n <= max ? n : null;
}
function normalizeStore(row) {
	const id = text$1(row, "bizesId");
	if (!id) throw Error("업소번호 확인 필요");
	if (text$1(row, "signguCd") !== "41150") throw Error("의정부시 조회 범위 검증 필요");
	const code = text$1(row, "adongCd").padEnd(10, "0");
	const region = COMMERCE_REGIONS.find((r) => r.id === code || r.name === text$1(row, "adongNm"));
	return {
		id,
		name: text$1(row, "bizesNm") || "상호명 미제공",
		branch: text$1(row, "brchNm"),
		dong: region?.id ?? code,
		dongName: region?.name ?? text$1(row, "adongNm"),
		largeCode: text$1(row, "indsLclsCd"),
		large: text$1(row, "indsLclsNm") || "업종 미분류",
		middleCode: text$1(row, "indsMclsCd"),
		middle: text$1(row, "indsMclsNm") || "업종 미분류",
		smallCode: text$1(row, "indsSclsCd"),
		small: text$1(row, "indsSclsNm") || "업종 미분류",
		address: text$1(row, "rdnmAdr") || text$1(row, "lnoAdr"),
		lon: coordinate(row.lon, 126, 129),
		lat: coordinate(row.lat, 36, 39)
	};
}
function industries(stores, level) {
	const groups = /* @__PURE__ */ new Map();
	for (const s of stores) {
		const code = s[`${level}Code`] || "unknown", old = groups.get(code);
		if (old) old.count++;
		else groups.set(code, {
			code,
			name: s[level],
			count: 1
		});
	}
	return [...groups.values()].sort((a, b) => b.count - a.count || a.code.localeCompare(b.code));
}
function summarizeCommerce(stores, runId, collectedAt, expected, previous) {
	if (new Set(stores.map((s) => s.id)).size !== stores.length || stores.length !== expected) throw Error("전체 페이지·업소번호 중복 검증 필요");
	const regions = COMMERCE_REGIONS.map((r) => {
		const local = r.id === "city" ? stores : stores.filter((s) => s.dong === r.id);
		return {
			id: r.id,
			name: r.name,
			count: local.length,
			food: local.filter(isFood).length,
			convenience: local.filter((s) => convenienceKind(s) !== null).length,
			mapped: local.filter((s) => s.lon !== null && s.lat !== null).length,
			industries: industries(local, "large"),
			middle: industries(local, "middle"),
			small: industries(local, "small"),
			services: CONVENIENCE.map((c) => ({
				id: c.id,
				name: c.name,
				count: local.filter((s) => convenienceKind(s) === c.id).length
			}))
		};
	});
	const unmapped = stores.filter((s) => !COMMERCE_REGIONS.some((r) => r.id === s.dong)).length;
	if (regions.slice(1).reduce((n, r) => n + r.count, 0) + unmapped !== expected) throw Error("동별 합계 검증 필요");
	for (const r of regions) if (r.industries.reduce((n, i) => n + i.count, 0) !== r.count) throw Error("업종별 합계 검증 필요");
	const date = new Date(new Date(collectedAt).getTime() + 9 * 36e5).toISOString().slice(0, 10);
	const history = [...(previous?.history ?? []).filter((h) => h.date !== date), {
		date,
		count: expected
	}].slice(-12);
	return {
		analysisVersion: 3,
		runId,
		collectedAt,
		sourceTotal: expected,
		count: expected,
		regions,
		unmapped,
		missingCoordinates: stores.filter((s) => s.lon === null || s.lat === null).length,
		history
	};
}
//#endregion
//#region lib/commerce-store.ts
var DAY$1 = 864e5;
function db() {
	if (!env.DB) throw Error("상권 DB 연결 확인 필요");
	return env.DB;
}
async function readJob() {
	const r = await db().prepare("SELECT payload FROM commerce_state WHERE id='uijeongbu'").first();
	return r ? JSON.parse(r.payload) : null;
}
async function saveJob(j) {
	await db().prepare("INSERT INTO commerce_state(id,payload,saved_at) VALUES('uijeongbu',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(JSON.stringify(j), Date.now()).run();
}
async function readSnapshot() {
	const r = await db().prepare("SELECT payload FROM commerce_snapshot WHERE id='uijeongbu'").first();
	return r ? JSON.parse(r.payload) : null;
}
function status(j) {
	return {
		pending: j?.status === "pending",
		page: j ? j.nextPage - 1 : 0,
		pages: j?.total !== null && j?.total !== void 0 ? Math.ceil(j.total / 1e3) : null,
		rows: j?.rows ?? 0,
		total: j?.total ?? null,
		error: j?.error ?? null,
		startedAt: j?.startedAt ?? null
	};
}
async function loadCommerce() {
	let [snapshot, j, pop] = await Promise.all([
		readSnapshot(),
		readJob(),
		readPopulation().catch(() => null)
	]);
	if (snapshot && snapshot.analysisVersion !== 3) {
		const revised = summarizeCommerce((await db().prepare("SELECT payload FROM commerce_pages WHERE run_id=? ORDER BY page").bind(snapshot.runId).all()).results.flatMap((p) => JSON.parse(p.payload)), snapshot.runId, snapshot.collectedAt, snapshot.sourceTotal, snapshot);
		snapshot = (await db().prepare("UPDATE commerce_snapshot SET payload=? WHERE id='uijeongbu' AND json_extract(payload,'$.runId')=?").bind(JSON.stringify(revised), snapshot.runId).run()).meta.changes ? revised : await readSnapshot();
	}
	return {
		snapshot,
		status: status(j),
		population: pop?.population.find((m) => m.complete) ?? null
	};
}
async function provider$1(page) {
	const key = env.COMMERCE_API_KEY;
	if (!key) throw Error("상권 API 인증 설정 확인 필요");
	const u = new URL("https://apis.data.go.kr/B553077/api/open/sdsc2/storeListInDong");
	let decoded = key;
	try {
		decoded = decodeURIComponent(key);
	} catch {}
	for (const [k, v] of Object.entries({
		serviceKey: decoded,
		divId: "signguCd",
		key: "41150",
		numOfRows: "1000",
		pageNo: String(page),
		type: "json"
	})) u.searchParams.set(k, v);
	const r = await fetch(u, { signal: AbortSignal.timeout(18e3) });
	if (!r.ok) throw Error("상권 제공기관 응답 지연");
	let json;
	try {
		json = await r.json();
	} catch {
		throw Error("상권 제공기관 응답 형식 확인 필요");
	}
	const root = json.response ?? json;
	const code = String(root.header?.resultCode ?? root.header?.resultcode ?? "");
	if (![
		"00",
		"0",
		""
	].includes(code)) throw Error(`상권 제공기관 응답 코드 ${code.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 20)}`);
	const body = root.body;
	if (!body) throw Error("상권 응답 본문 확인 필요");
	const total = Number(body.totalCount);
	if (!Number.isInteger(total) || total < 0) throw Error("전체 업소 수 확인 필요");
	return {
		total,
		stores: (Array.isArray(body.items) ? body.items : body.items?.item ? Array.isArray(body.items.item) ? body.items.item : [body.items.item] : []).map(normalizeStore)
	};
}
async function collectCommerce(force = false) {
	const now = Date.now(), previous = await readSnapshot();
	let j = await readJob();
	if (j?.leaseUntil && j.leaseUntil > now) return status(j);
	const retry = await readApiCache("commerce-retry-v2");
	if (retry && retry.due > now) return status(j);
	if (force && j?.error) j = null;
	if (j?.status !== "pending") {
		if (!force && previous && now - Date.parse(previous.collectedAt) < DAY$1) return status(j);
		j = {
			runId: crypto.randomUUID(),
			nextPage: 1,
			total: null,
			rows: 0,
			status: "pending",
			startedAt: new Date(now).toISOString(),
			leaseUntil: 0,
			error: null
		};
		if (!(await db().prepare("INSERT INTO commerce_state(id,payload,saved_at) VALUES('uijeongbu',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at WHERE CAST(json_extract(commerce_state.payload,'$.leaseUntil') AS INTEGER)<=?").bind(JSON.stringify({
			...j,
			leaseUntil: now + 5e4
		}), now, now).run()).meta.changes) return status(await readJob());
	} else if (!(await db().prepare("UPDATE commerce_state SET payload=?,saved_at=? WHERE id='uijeongbu' AND CAST(json_extract(payload,'$.leaseUntil') AS INTEGER)<=?").bind(JSON.stringify({
		...j,
		leaseUntil: now + 5e4
	}), now, now).run()).meta.changes) return status(await readJob());
	j.leaseUntil = now + 5e4;
	j.error = null;
	try {
		for (let n = 0; n < 2; n++) {
			const page = await provider$1(j.nextPage);
			if (j.total !== null && j.total !== page.total) throw Error("수집 중 제공기관 전체 건수 변경 · 재수집 확인 필요");
			j.total = page.total;
			const expected = Math.min(1e3, Math.max(0, page.total - (j.nextPage - 1) * 1e3));
			if (page.stores.length !== expected) throw Error("페이지 수집 건수 확인 필요");
			await db().prepare("INSERT INTO commerce_pages(id,run_id,page,payload) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload").bind(`${j.runId}:${j.nextPage}`, j.runId, j.nextPage, JSON.stringify(page.stores)).run();
			j.rows += page.stores.length;
			j.nextPage++;
			if (j.rows === j.total) {
				const snapshot = summarizeCommerce((await db().prepare("SELECT payload FROM commerce_pages WHERE run_id=? ORDER BY page").bind(j.runId).all()).results.flatMap((p) => JSON.parse(p.payload)), j.runId, (/* @__PURE__ */ new Date()).toISOString(), j.total, previous);
				await db().prepare("INSERT INTO commerce_snapshot(id,payload,saved_at) VALUES('uijeongbu',?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,saved_at=excluded.saved_at").bind(JSON.stringify(snapshot), Date.now()).run();
				j.status = "complete";
				await db().prepare("DELETE FROM commerce_pages WHERE run_id NOT IN (?,?)").bind(j.runId, previous?.runId ?? j.runId).run();
				break;
			}
		}
	} catch (e) {
		j.error = e instanceof Error ? e.message : "상권 자료 수집 지연";
	}
	if (j.error) {
		const attempt = retry?.attempt ?? 0;
		await writeApiCache("commerce-retry-v2", {
			attempt: attempt + 1,
			due: Date.now() + retryDelay(attempt)
		}, 86400);
	} else await writeApiCache("commerce-retry-v2", {
		attempt: 0,
		due: 0
	}, 86400);
	j.leaseUntil = 0;
	await saveJob(j);
	return status(j);
}
//#endregion
//#region lib/shared-refresh.ts
async function claimRefresh(id, leaseMs = 12e4) {
	if (!env.DB) throw Error("자료 저장소 연결 확인 필요");
	const now = Date.now();
	return (await env.DB.prepare("INSERT INTO collection_state(id,next_due,lease_until,last_started) VALUES(?,0,?,?) ON CONFLICT(id) DO UPDATE SET lease_until=excluded.lease_until,last_started=excluded.last_started WHERE collection_state.lease_until<=? AND collection_state.next_due<=?").bind(id, now + leaseMs, now, now, now).run()).meta.changes ? now : null;
}
async function finishRefresh(id, started, due, error = null) {
	if (!env.DB) throw Error("자료 저장소 연결 확인 필요");
	await env.DB.prepare("UPDATE collection_state SET lease_until=0,next_due=?,last_finished=?,last_error=? WHERE id=? AND last_started=?").bind(due, Date.now(), error, id, started).run();
}
async function monthlyCached(key, ttl, load, empty) {
	const cached = await readApiCache(key);
	if (cached) return cached;
	const started = await claimRefresh(`cache:${key}`, 45e4);
	const good = await readApiCache(`${key}:last-good`);
	if (!started) return good ? {
		...good,
		stale: true
	} : {
		...empty,
		error: "자료 수집 중 또는 재시도 대기 · 저장자료 유지"
	};
	try {
		const value = await load();
		if (value.error) throw Error(value.error);
		await writeApiCache(key, value, ttl);
		await writeApiCache(`${key}:last-good`, value, 366 * 86400);
		await writeApiCache(`${key}:retry`, { attempt: 0 }, 7 * 86400);
		await finishRefresh(`cache:${key}`, started, Date.now() + ttl * 1e3);
		return value;
	} catch (e) {
		const attempt = (await readApiCache(`${key}:retry`))?.attempt ?? 0, delay = retryDelay(attempt);
		const error = e instanceof Error ? e.message : "자료 제공기관 연결 지연";
		const value = {
			...good ?? empty,
			error,
			stale: !!good
		};
		await writeApiCache(`${key}:retry`, { attempt: attempt + 1 }, 7 * 86400);
		await writeApiCache(key, value, delay / 1e3);
		await finishRefresh(`cache:${key}`, started, Date.now() + delay, error);
		return value;
	}
}
//#endregion
//#region lib/home-summary.ts
async function refreshHomeSummary() {
	const started = await claimRefresh("home-summary-v2", 3e4);
	if (!started) return;
	try {
		if (!env.DB) throw Error("자료 저장소 연결 확인 필요");
		const [pop, row, weather] = await Promise.all([
			readPopulation(),
			env.DB.prepare("SELECT payload FROM commerce_snapshot WHERE id='uijeongbu'").first(),
			readSnapshot$1("city")
		]);
		const commerce = row ? JSON.parse(row.payload) : null;
		const summary = {
			population: pop ? {
				population: pop.population.map((m) => ({
					...m,
					regions: m.regions.map(({ areas, ...r }) => r)
				})),
				collectedAt: pop.populationSavedAt,
				pending: pop.pending
			} : null,
			commerce: commerce ? {
				count: commerce.count,
				regions: commerce.regions.map((r) => ({
					id: r.id,
					name: r.name,
					count: r.count,
					industries: r.industries
				})),
				collectedAt: commerce.collectedAt
			} : null,
			weather: weather ? {
				current: weather.current,
				hours: weather.hours,
				observedAt: weather.observedAt,
				air: weather.air,
				fetchedAt: weather.fetchedAt,
				grid: weather.grid
			} : null,
			summarySavedAt: (/* @__PURE__ */ new Date()).toISOString()
		};
		await writeApiCache("home-summary:v2", summary, 300);
		await writeApiCache("home-summary:v2:last-good", summary, 366 * 86400);
		await finishRefresh("home-summary-v2", started, 0);
		return summary;
	} catch (e) {
		await finishRefresh("home-summary-v2", started, Date.now() + 6e4, "홈 요약 저장 지연");
		throw e;
	}
}
var building;
async function readHomeSummary() {
	const saved = await readApiCache("home-summary:v2");
	if (saved) return saved;
	if (!building) building = (async () => await refreshHomeSummary() ?? await readApiCache("home-summary:v2:last-good") ?? {
		population: null,
		commerce: null,
		weather: null
	})().finally(() => {
		building = void 0;
	});
	return building;
}
//#endregion
//#region lib/traffic.ts
var TRAFFIC_CITY_CODES = [
	["가평군", "1322"],
	["고양시", "1318"],
	["과천시", "1332"],
	["광명시", "1309"],
	["광주시", "1319"],
	["구리시", "1310"],
	["군포시", "1333"],
	["김포시", "1327"],
	["남양주시", "1334"],
	["동두천시", "1330"],
	["부천시", "1306"],
	["성남시", "1303"],
	["수원시", "1302"],
	["시흥시", "1316"],
	["안산시", "1307"],
	["안성시", "1326"],
	["안양시", "1305"],
	["양주시", "1311"],
	["양평군", "1323"],
	["여주시", "1313"],
	["연천군", "1320"],
	["오산시", "1335"],
	["용인시", "1325"],
	["의왕시", "1336"],
	["의정부시", "1304"],
	["이천시", "1324"],
	["파주시", "1317"],
	["평택시", "1308"],
	["포천시", "1321"],
	["하남시", "1337"],
	["화성시", "1315"]
];
var TRAFFIC_CITIES = TRAFFIC_CITY_CODES.map(([city]) => city);
var VIOLATIONS = [
	["cnt_027_01", "과속"],
	["cnt_027_02", "중앙선 침범"],
	["cnt_027_03", "신호위반"],
	["cnt_027_04", "안전거리 미확보"],
	["cnt_027_05", "안전운전 의무 불이행"],
	["cnt_027_06", "교차로 통행방법 위반"],
	["cnt_027_07", "보행자 보호의무 위반"],
	["cnt_027_99", "기타"]
];
var COLLISIONS = [
	["cnt_014_01", "차대사람"],
	["cnt_014_02", "차대차"],
	["cnt_014_03", "차량단독"],
	["cnt_014_04", "철길건널목"]
];
var trafficYears = () => Array.from({ length: 5 }, (_, i) => Number(new Intl.DateTimeFormat("en", {
	year: "numeric",
	timeZone: "Asia/Seoul"
}).format(/* @__PURE__ */ new Date())) - 1 - i);
function numeric(v) {
	if (v === null || v === void 0 || String(v).trim() === "" || String(v).trim() === "-") return null;
	const n = Number(String(v).replaceAll(",", ""));
	return Number.isFinite(n) && n >= 0 ? n : null;
}
function normalizeTraffic(raw, year) {
	const city = TRAFFIC_CITIES.find((c) => String(raw.sido_sgg_nm ?? "").trim().endsWith(c));
	if (!city) return null;
	if (Number(raw.std_year) !== year) throw Error("교통사고 통계 기준연도 확인 필요");
	let category = String(raw.acc_cl_nm ?? "").replace(/노인/g, "고령").replace("어린이보호구역내어린이사고", "스쿨존내어린이사고").replace("어린이보호구역내 어린이사고", "스쿨존내어린이사고");
	if (category === "PM사고") category = "개인형이동수단(PM)사고";
	const accidents = numeric(raw.acc_cnt), deaths = numeric(raw.dth_dnv_cnt), injuries = numeric(raw.injpsn_cnt);
	if (!category || accidents === null || deaths === null || injuries === null) throw Error("교통사고 통계 필수 항목 확인 필요");
	return {
		year,
		city,
		category,
		accidents,
		deaths,
		injuries,
		fatality: numeric(raw.ftlt_rate),
		accidentShare: numeric(raw.acc_cnt_cmrt),
		deathShare: numeric(raw.dth_dnv_cnt_cmrt),
		injuryShare: numeric(raw.injpsn_cnt_cmrt),
		nationalAccidents: numeric(raw.tot_acc_cnt),
		nationalDeaths: numeric(raw.tot_dth_dnv_cnt),
		nationalInjuries: numeric(raw.tot_injpsn_cnt),
		perPopulation: numeric(raw.pop_100k),
		perCar: numeric(raw.car_10k),
		details: Object.fromEntries([...VIOLATIONS, ...COLLISIONS].map(([key]) => [key, numeric(raw[key])]))
	};
}
//#endregion
//#region lib/traffic-store.ts
var cacheKey = (year) => `traffic:${year}:last-good`;
function nextQuarter(now = Date.now()) {
	const d = new Date(now + 9 * 36e5);
	return Date.UTC(d.getUTCFullYear(), Math.floor(d.getUTCMonth() / 3) * 3 + 3, 1) - 9 * 36e5;
}
async function checkNewTraffic() {
	const latest = (await readTraffic()).snapshots[0], candidate = trafficYears()[0];
	if (latest && latest.year >= candidate) return {
		pending: false,
		checked: true,
		newYear: false,
		nextCheck: nextQuarter()
	};
	const id = "traffic-new-year-quarterly", started = await claimRefresh(id, 45e3);
	if (!started) return { pending: false };
	try {
		const snapshot = await provider(candidate);
		await writeApiCache(cacheKey(candidate), snapshot, 366 * 86400 * 10);
		await finishRefresh(id, started, nextQuarter());
		return {
			pending: false,
			saved: true,
			year: candidate
		};
	} catch {
		await finishRefresh(id, started, nextQuarter());
		return {
			pending: false,
			newYear: false,
			message: "신규 연도 통계 미확인 · 다음 분기에 다시 확인"
		};
	}
}
async function readTraffic() {
	const years = trafficYears();
	if (!env.DB) return {
		years,
		snapshots: [],
		states: []
	};
	const [cache, state] = await Promise.all([env.DB.prepare("SELECT payload FROM api_cache WHERE cache_key LIKE 'traffic:%:last-good'").all(), env.DB.prepare("SELECT id,next_due,lease_until,last_error FROM collection_state WHERE id LIKE 'traffic:v5:%'").all()]);
	return {
		years,
		snapshots: cache.results.map((r) => JSON.parse(r.payload)).sort((a, b) => b.year - a.year).slice(0, 5),
		states: state.results.map((r) => ({
			year: Number(r.id.split(":")[2]),
			nextDue: r.next_due,
			error: r.last_error,
			collecting: r.lease_until > Date.now()
		})).filter((s) => years.includes(s.year))
	};
}
async function providerCity(year, city, regionCode) {
	const key = env.TRAFFIC_API_KEY;
	if (!key) throw Error("교통사고 API 인증 설정 확인 필요");
	let decoded = key;
	try {
		decoded = decodeURIComponent(key);
	} catch {}
	const u = new URL("https://apis.data.go.kr/B552061/lgStat/getRestLgStat");
	u.search = new URLSearchParams({
		ServiceKey: decoded,
		searchYearCd: String(year),
		siDo: "1300",
		guGun: regionCode,
		type: "json",
		numOfRows: "100",
		pageNo: "1"
	}).toString();
	let response;
	try {
		response = await fetch(u, { signal: AbortSignal.timeout(2e4) });
	} catch {
		throw Error("교통사고 제공기관 연결 지연");
	}
	if (!response.ok) throw Error(`교통사고 제공기관 응답 지연 (${response.status})`);
	let json;
	try {
		json = await response.json();
	} catch {
		throw Error("교통사고 제공기관 응답 형식 확인 필요");
	}
	const root = json.response ?? json, code = String(root.header?.resultCode ?? root.resultCode ?? "");
	if (!["00", "0"].includes(code)) throw Error(code === "03" ? "해당 연도 통계가 아직 제공되지 않습니다." : `교통사고 API 응답 코드 ${code.replace(/[^A-Za-z0-9_-]/g, "").slice(0, 15) || "확인 필요"}`);
	const body = root.body ?? root;
	const items = body.items?.item ?? body.items ?? [];
	const raw = Array.isArray(items) ? items : [items];
	const total = Number(body.totalCount);
	if (!Number.isInteger(total) || total !== raw.length || total > 1e3) throw Error("교통사고 통계 전체 건수 확인 필요");
	const rows = raw.map((r) => normalizeTraffic(r, year)).filter((r) => r !== null);
	if (new Set(rows.map((r) => `${r.city}:${r.category}`)).size !== rows.length) throw Error("교통사고 통계 중복 항목 확인 필요");
	if (!rows.some((r) => r.city === city && r.category === "전체사고")) throw Error("의정부시 해당 연도 통계가 아직 제공되지 않습니다.");
	return {
		year,
		collectedAt: (/* @__PURE__ */ new Date()).toISOString(),
		rows
	};
}
async function provider(year) {
	const results = [];
	for (let i = 0; i < TRAFFIC_CITY_CODES.length; i += 4) results.push(...await Promise.all(TRAFFIC_CITY_CODES.slice(i, i + 4).map(([city, code]) => providerCity(year, city, code))));
	return {
		year,
		collectedAt: (/* @__PURE__ */ new Date()).toISOString(),
		rows: results.flatMap((r) => r.rows)
	};
}
//#endregion
//#region lib/resident-age.ts
function normalizeResidentAge(rows, fields) {
	return rows.map((raw) => {
		const row = Object.fromEntries(Object.entries(raw).map(([k, v]) => [k.trim(), v]));
		const id = String(row.admmCd ?? "");
		if (!POP_REGIONS.some((r) => r.id === id) && id !== "4115000000") return null;
		const cells = [];
		const ageKeys = Object.keys(row).flatMap((k) => {
			const m = k.match(/(?:male|feml)(\d{1,3})Age/);
			return m ? [Number(m[1])] : [];
		});
		const decadeKeys = ageKeys.length === 22 && ageKeys.every((a) => a % 10 === 0);
		const lastAge = Math.max(100, ...Object.keys(row).map((k) => Number(k.match(/(?:male|feml)(\d{1,3})Age/)?.[1] ?? 0)));
		for (const [key, value] of Object.entries(row)) {
			const desc = (fields[key] ?? key).replace(/[\s()]/g, "");
			let sex = /남자|male/i.test(desc) ? "m" : /여자|feml|female/i.test(desc) ? "f" : "";
			if (/여자|feml|female/i.test(desc)) sex = "f";
			const range = desc.match(/(\d{1,3})(?:세)?[~∼–-](\d{1,3})세/) ?? key.match(/(?:male|feml)(\d{1,3})(?:To|to|_)(\d{1,3})/);
			const single = desc.match(/(\d{1,3})세/) ?? key.match(/(?:male|feml)(\d{1,3})Age/);
			if (!sex || !range && !single) continue;
			const from = Number((range ?? single)[1]), to = range ? Number(range[2]) : /이상|Above|Over/i.test(desc) || from === lastAge ? 110 : decadeKeys ? from + 9 : from;
			const n = numeric$1(value);
			if (n === null) throw Error("연령별 인구 누락 항목 확인 필요");
			cells.push({
				from,
				to,
				sex,
				n
			});
		}
		if (!cells.length) throw Error("연령별 인구 제공 항목 확인 필요");
		for (const sex of ["m", "f"]) {
			const coverage = Array(111).fill(0);
			cells.filter((c) => c.sex === sex).forEach((c) => {
				for (let a = c.from; a <= Math.min(c.to, 110); a++) coverage[a]++;
			});
			if (coverage.some((n) => n !== 1)) throw Error(`연령 구간의 누락·중복 확인 필요 (${sex}, ${cells.filter((c) => c.sex === sex).length}개 구간, ${coverage.filter((n) => n === 0).length}개 연령 누락)`);
		}
		const total = numeric$1(row.totNmprCnt ?? row.totPpltnCnt);
		if (total === null || cells.reduce((s, c) => s + c.n, 0) !== total) throw Error("연령별 합계와 총인구 확인 필요");
		const groups = Array.from({ length: 10 }, (_, i) => {
			const from = i * 10, to = i === 9 ? 110 : from + 9;
			const c = cells.filter((c) => c.from >= from && c.to <= to);
			return {
				label: i === 9 ? "90세 이상" : `${from}~${to}세`,
				male: c.filter((c) => c.sex === "m").reduce((s, c) => s + c.n, 0),
				female: c.filter((c) => c.sex === "f").reduce((s, c) => s + c.n, 0)
			};
		});
		if (groups.reduce((s, c) => s + c.male + c.female, 0) !== total) throw Error("10세별 집계 구간 확인 필요");
		const limits = [
			{
				label: "유소년 · 0~14세",
				from: 0,
				to: 14
			},
			{
				label: "생산연령 · 15~64세",
				from: 15,
				to: 64
			},
			{
				label: "고령 · 65세 이상",
				from: 65,
				to: 110
			}
		];
		const exact = limits.every((g) => !cells.some((c) => c.from < g.from && c.to >= g.from || c.from <= g.to && c.to > g.to));
		return {
			id: id === "4115000000" ? "city" : id,
			total,
			groups,
			summary: exact ? limits.map((g) => ({
				label: g.label,
				count: cells.filter((c) => c.from >= g.from && c.to <= g.to).reduce((s, c) => s + c.n, 0)
			})) : null
		};
	}).filter((r) => Boolean(r));
}
//#endregion
//#region age-route.ts
var active$1 = /* @__PURE__ */ new Map();
async function GET$1(request) {
	const month = new URL(request.url).searchParams.get("month") ?? "";
	if (!/^20\d{2}(0[1-9]|1[0-2])$/.test(month)) return Response.json({ error: "통계 기준월 확인 필요" }, { status: 400 });
	const key = `resident-age:v2:${month}`;
	const cached = await readApiCache(key);
	if (cached) return Response.json(cached);
	if (!active$1.has(key)) active$1.set(key, monthlyCached(key, monthlyTtl(month, recentMonths()[0]), async () => {
		const [schema, result] = await Promise.all([contract("residentAge"), providerPage("residentAge", {
			type: "json",
			numOfRows: "100",
			pageNo: "1",
			lv: "3",
			admmCd: "4115000000",
			srchFrYm: month,
			srchToYm: month
		})]);
		if (!result.rows.length || result.total !== null && result.total !== result.rows.length) throw Error("연령별 주민등록 자료 제공 범위 확인 필요");
		let regions;
		try {
			regions = normalizeResidentAge(result.rows, schema.fields);
		} catch (e) {
			return {
				month,
				regions: [],
				error: e instanceof Error ? e.message : "연령별 자료 확인 필요",
				sourceFields: Object.keys(result.rows[0]),
				sourceDescriptions: schema.fields
			};
		}
		const dongs = regions.filter((r) => r.id !== "city");
		if (dongs.length !== 15) throw Error("15개 동 연령별 인구 수집 확인 필요");
		if (!regions.some((r) => r.id === "city")) {
			const city = {
				id: "city",
				total: dongs.reduce((s, r) => s + r.total, 0),
				groups: dongs[0].groups.map((g, i) => ({
					label: g.label,
					male: dongs.reduce((s, r) => s + r.groups[i].male, 0),
					female: dongs.reduce((s, r) => s + r.groups[i].female, 0)
				})),
				summary: dongs.every((r) => r.summary) ? dongs[0].summary.map((g, i) => ({
					label: g.label,
					count: dongs.reduce((s, r) => s + r.summary[i].count, 0)
				})) : null
			};
			regions.unshift(city);
		}
		return {
			month,
			regions,
			savedAt: (/* @__PURE__ */ new Date()).toISOString()
		};
	}, {
		month,
		regions: [],
		error: void 0
	}).finally(() => active$1.delete(key)));
	return Response.json(await active$1.get(key));
}
//#endregion
//#region lib/household-sizes.ts
var clean = (s) => s.replace(/[\s()·_]/g, "");
function normalizeHouseholdSizes(rows, month, fields = {}) {
	const result = /* @__PURE__ */ new Map();
	for (const raw of rows) {
		const row = Object.fromEntries(Object.entries(raw).map(([k, v]) => [k.trim(), v]));
		const find = (aliases, labels) => {
			const described = Object.entries(fields).find(([, v]) => labels.includes(clean(v)))?.[0]?.trim();
			return described && described in row ? row[described] : row[aliases.find((k) => k in row) ?? ""];
		};
		if (String(find(["statsYm", "statsYymm"], ["통계년월"]) ?? "").replace(/\D/g, "") !== month) throw Error("요청 기준월과 세대 자료의 기준월이 다릅니다.");
		const city = String(find(["sggNm"], ["시군구명"]) ?? "");
		if (city && city !== "의정부시") continue;
		const code = String(find(["admmCd", "admmInstCd"], ["행정기관코드"]) ?? "");
		const dong = String(find(["dongNm", "admmNm"], ["행정동명"]) ?? "");
		const region = POP_REGIONS.find((r) => r.id === code || r.name === dong) || (code === "4115000000" ? POP_REGIONS[0] : void 0);
		if (!region) continue;
		if ([
			"tong",
			"ban",
			"tongNo",
			"banNo"
		].some((k) => row[k] !== void 0 && !/^(|0+|계|합계|전체)$/.test(String(row[k]).trim()))) throw Error("동 총계 대신 통·반 자료가 수신되어 집계를 보류합니다.");
		const total = numeric$1(find([
			"hhCnt",
			"totHhCnt",
			"totalHhCnt"
		], [
			"전체세대수",
			"총세대수",
			"세대수"
		]));
		const sizes = Array.from({ length: 10 }, (_, i) => {
			const n = i + 1;
			return numeric$1(find([
				`hhMbr${n}HhCnt`,
				`hhMbr${n}Cnt`,
				`hsmb${n}HhCnt`,
				`hsmb${n}Cnt`,
				`hh${n}Cnt`,
				`${n}psnHhCnt`,
				`psn${n}HhCnt`,
				`hhMbr${n}AbvHhCnt`
			], n === 10 ? [
				"10인세대이상",
				"10인이상세대수",
				"10인이상세대",
				"10인세대",
				"10인세대수"
			] : [`${n}인세대`, `${n}인세대수`]));
		});
		if (total === null || sizes.some((v) => v === null)) throw Error("세대원수별 수치 항목을 확인하고 있습니다.");
		const counts = sizes;
		if (counts.reduce((s, v) => s + v, 0) !== total) throw Error("세대원수별 합계와 전체 세대수가 일치하지 않아 표시를 보류합니다.");
		const value = {
			id: region.id,
			name: region.name,
			total,
			sizes: counts
		};
		const prev = result.get(region.id);
		if (prev && JSON.stringify(prev) !== JSON.stringify(value)) throw Error("동별 세대 자료가 중복되어 집계를 보류합니다.");
		result.set(region.id, value);
	}
	const dongs = POP_REGIONS.filter((r) => r.id !== "city").map((r) => result.get(r.id));
	if (dongs.some((r) => !r)) throw Error("15개 동의 세대 자료가 모두 수신되지 않았습니다.");
	const total = dongs.reduce((s, r) => s + r.total, 0), sizes = Array.from({ length: 10 }, (_, i) => dongs.reduce((s, r) => s + r.sizes[i], 0));
	const official = result.get("city");
	if (official && (official.total !== total || official.sizes.some((v, i) => v !== sizes[i]))) throw Error("시 전체와 동별 세대수 합계가 일치하지 않습니다.");
	return [{
		id: "city",
		name: "의정부시 전체",
		total,
		sizes
	}, ...dongs];
}
//#endregion
//#region household-route.ts
var active = /* @__PURE__ */ new Map();
async function collect(month) {
	return monthlyCached(`household-sizes:v1:${month}`, monthlyTtl(month, recentMonths()[0]), async () => {
		let key = env.HOUSEHOLD_SIZE_API_KEY ?? env.POPULATION_API_KEY;
		if (!key) throw Error("세대원수별 API 연결 설정 확인 필요");
		try {
			key = decodeURIComponent(key);
		} catch {}
		let fields = await readApiCache("household-size-contract:v1");
		if (!fields) try {
			const r = await fetch("https://www.data.go.kr/data/15108081/openapi.do", { signal: AbortSignal.timeout(8e3) });
			if (r.ok) {
				const c = extractContract(await r.text());
				if (c) {
					fields = c.fields;
					await writeApiCache("household-size-contract:v1", fields, 604800);
				}
			}
		} catch {}
		const rows = [];
		let expected = null;
		for (let page = 1; page <= 20; page++) {
			const url = new URL("https://apis.data.go.kr/1741000/admmHsmbHh/selectAdmmHsmbHh");
			for (const [k, v] of Object.entries({
				serviceKey: key,
				type: "json",
				numOfRows: "100",
				pageNo: String(page),
				lv: "3",
				admmCd: "4115000000",
				srchFrYm: month,
				srchToYm: month
			})) url.searchParams.set(k, v);
			const response = await fetch(url, { signal: AbortSignal.timeout(2e4) });
			if (!response.ok) throw Error("세대원수별 자료 제공기관 연결 지연");
			let json;
			try {
				json = await response.json();
			} catch {
				throw Error("세대원수별 API 응답 형식 확인 필요");
			}
			const result = rowsAndCount(json);
			if (result.total !== null) expected = result.total;
			rows.push(...result.rows);
			if (expected !== null && rows.length >= expected || result.rows.length < 100) break;
		}
		if (!rows.length) throw Error("선택한 기준월의 세대 자료가 아직 제공되지 않습니다.");
		if (expected !== null && expected !== rows.length) throw Error("세대 자료의 일부 페이지가 수신되지 않았습니다.");
		let regions;
		try {
			regions = normalizeHouseholdSizes(rows, month, fields);
		} catch (e) {
			console.warn("household-size schema", JSON.stringify({
				fields: fields ?? {},
				sample: rows[0]
			}));
			throw e;
		}
		return {
			month,
			regions,
			savedAt: (/* @__PURE__ */ new Date()).toISOString()
		};
	}, {
		month,
		regions: []
	});
}
async function GET(request) {
	const month = new URL(request.url).searchParams.get("month") ?? "";
	if (!/^20\d{2}(0[1-9]|1[0-2])$/.test(month)) return Response.json({ error: "통계 기준월 확인 필요" }, { status: 400 });
	if (!active.has(month)) active.set(month, collect(month).finally(() => active.delete(month)));
	return Response.json(await active.get(month), { headers: { "Cache-Control": "no-store" } });
}
var DAY = 864e5;
function text(value) {
	return (typeof value === "object" && value !== null ? String(value["#text"] ?? "") : String(value ?? "")).replace(/<[^>]*>/g, "").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&quot;/g, "\"").replace(/&#39;|&apos;/g, "'").replace(/\s+/g, " ").trim();
}
function safeNewsUrl(value) {
	try {
		const url = new URL(String(value));
		return url.protocol === "https:" && url.hostname === "news.google.com" && !url.username && !url.password && /^\/(rss\/)?articles\//.test(url.pathname) ? url.href : null;
	} catch {
		return null;
	}
}
function safePressUrl(value) {
	try {
		const url = new URL(String(value), "https://www.ui4u.go.kr");
		if (!["http:", "https:"].includes(url.protocol) || !["ui4u.go.kr", "www.ui4u.go.kr"].includes(url.hostname) || url.username || url.password || !url.pathname.startsWith("/portal/")) return null;
		url.protocol = "https:";
		return url.href;
	} catch {
		return null;
	}
}
function articleId(url) {
	let a = 2166136261, b = 5381;
	for (const c of url) {
		a = Math.imul(a ^ c.charCodeAt(0), 16777619);
		b = Math.imul(b, 33) ^ c.charCodeAt(0);
	}
	return (a >>> 0).toString(16).padStart(8, "0") + (b >>> 0).toString(16).padStart(8, "0");
}
function recentArticles(articles, now = Date.now()) {
	return articles.filter((a) => {
		const date = Date.parse(a.publishedAt);
		return Number.isFinite(date) && date >= now - 7 * DAY && date <= now + 15 * 6e4;
	}).sort((a, b) => Date.parse(b.publishedAt) - Date.parse(a.publishedAt));
}
function mergeNewsArticles(incoming, saved, now = Date.now()) {
	const unique = /* @__PURE__ */ new Map();
	for (const article of [...incoming, ...saved]) if (!unique.has(article.url)) unique.set(article.url, article);
	return recentArticles([...unique.values()], now).slice(0, 150);
}
function tokens(title) {
	return new Set(title.toLowerCase().replace(/[^가-힣a-z0-9\s]/g, " ").split(/\s+/).filter((s) => s.length > 1));
}
function similar(a, b) {
	const normalize = (v) => v.replace(/[^가-힣a-zA-Z0-9]/g, "").toLowerCase();
	if (normalize(a) === normalize(b)) return true;
	const x = tokens(a), y = tokens(b);
	if (Math.min(x.size, y.size) < 5) return false;
	const intersection = [...x].filter((t) => y.has(t)).length;
	return intersection / (x.size + y.size - intersection) >= .72;
}
function parseNewsFeed(xml, now = Date.now(), kind = "news") {
	if (XMLValidator.validate(xml) !== true) throw Error("뉴스 응답 형식을 확인하지 못했습니다.");
	const channel = new XMLParser({
		ignoreAttributes: false,
		trimValues: true,
		isArray: (name) => name === "item"
	}).parse(xml)?.rss?.channel;
	if (!channel || typeof channel !== "object") throw Error("뉴스 목록 응답이 아닙니다.");
	const entries = channel.item ?? [];
	if (!Array.isArray(entries)) throw Error("뉴스 목록 형식을 확인하지 못했습니다.");
	const articles = [];
	for (const entry of entries) {
		const source = kind === "press" ? "의정부시 공식 보도자료" : text(entry.source) || "출처 미표기";
		let title = text(entry.title);
		if (source && title.endsWith(" - " + source)) title = title.slice(0, -source.length - 3).trim();
		const url = (kind === "press" ? safePressUrl : safeNewsUrl)(text(entry.link));
		const rawDate = text(entry.pubDate ?? entry["dc:date"] ?? entry.date);
		const dateOnly = rawDate.match(/^(\d{4})[.\/-](\d{1,2})[.\/-](\d{1,2})[.]?$/);
		const published = Date.parse(dateOnly ? `${dateOnly[1]}-${dateOnly[2].padStart(2, "0")}-${dateOnly[3].padStart(2, "0")}T00:00:00+09:00` : rawDate);
		if (!title || !url || !Number.isFinite(published) || kind === "news" && !/의정부/.test(title + " " + text(entry.description))) continue;
		articles.push({
			id: articleId(url),
			title,
			source,
			publishedAt: new Date(published).toISOString(),
			url,
			relatedCount: 0
		});
	}
	if (entries.length && !articles.length) throw Error("뉴스 항목의 제목·날짜·링크 확인이 필요합니다.");
	const grouped = [];
	const seen = /* @__PURE__ */ new Set();
	for (const article of recentArticles(articles, now)) {
		if (seen.has(article.url)) continue;
		seen.add(article.url);
		const match = kind === "news" ? grouped.find((a) => similar(a.title, article.title)) : void 0;
		if (match) match.relatedCount++;
		else grouped.push(article);
	}
	return grouped.slice(0, 150);
}
//#endregion
//#region collect.ts
var root = resolve(process.env.DID_REPO_ROOT ?? process.cwd());
var paths = {
	data: resolve(root, "web/data.json"),
	news: resolve(root, "web/news.json"),
	state: resolve(root, "collector/state.json"),
	lock: resolve(root, "collector/.collect.lock")
};
var original;
var previousErrors = /* @__PURE__ */ new Map();
var HOUR = 36e5;
24 * HOUR;
var nowISO = () => (/* @__PURE__ */ new Date()).toISOString();
var savedTime = (value) => Number.isFinite(Date.parse(value)) ? Date.parse(value) : 0;
async function put(table, id, value, time = Date.now(), column = "id") {
	if (value === null || value === void 0) return;
	await database.prepare(`INSERT OR IGNORE INTO ${table} (${column},payload,saved_at) VALUES (?,?,?)`).bind(id, JSON.stringify(value), time).run();
}
async function setInitialState(id, last, interval) {
	await database.prepare("INSERT OR IGNORE INTO collection_state(id,next_due,lease_until,last_started,last_finished,last_error) VALUES(?,?,0,?,?,NULL)").bind(id, last ? last + interval : 0, last, last || null).run();
}
async function initialSeed(d, n) {
	const weather = d.weather;
	if (weather?.sources) {
		await put("weather_snapshots", "city", weather, savedTime(weather.storage?.savedAt ?? weather.fetchedAt), "region_id");
		await setInitialState("weather", savedTime(weather.fetchedAt), HOUR);
	}
	if (d.warnings) {
		await put("warning_snapshot", "gyeonggi", d.warnings, savedTime(d.warnings.collectedAt));
		await setInitialState("warnings", savedTime(d.warnings.collectedAt), 30 * 6e4);
	}
	if (d.population?.population) await put("population_snapshot", "uijeongbu", d.population, savedTime(d.population.collectedAt));
	if (d.births?.months) await put("population_jobs", "births:snapshot:v1", d.births, savedTime(d.births.savedAt));
	if (d.commerce?.snapshot) await put("commerce_snapshot", "uijeongbu", d.commerce.snapshot, savedTime(d.commerce.snapshot.collectedAt));
	for (const t of d.traffic?.snapshots ?? []) await writeApiCache(`traffic:${t.year}:last-good`, t, 366 * 86400 * 10);
	for (const [month, v] of Object.entries(d.monthly ?? {})) for (const [field, prefix] of [["ages", "resident-age:v2:"], ["households", "household-sizes:v1:"]]) {
		const data = v[field];
		if (!data?.regions?.length || data.error) continue;
		const ttl = month >= recentMonths()[0] ? 86400 : 30 * 86400;
		await writeApiCache(prefix + month, data, ttl);
		await writeApiCache(prefix + month + ":last-good", data, 366 * 86400);
	}
	for (const [key, v] of Object.entries(d.areas ?? {})) {
		if (!v.row) continue;
		await put("population_jobs", "population-areas:" + key, {
			page: 1,
			done: true,
			rows: [],
			summary: v.row,
			savedAt: v.savedAt,
			error: v.error ?? null,
			note: v.note ?? null,
			total: v.row.areas?.length ?? 0
		});
	}
	if (d.home) await writeApiCache("home-summary:v2:last-good", d.home, 366 * 86400);
	await setInitialState("news", savedTime(n?.lastAttemptAt ?? n?.collectedAt), HOUR);
}
async function state(id) {
	return await database.prepare("SELECT * FROM collection_state WHERE id=?").bind(id).first();
}
async function due(id, maxWait = 0) {
	const s = await state(id);
	if (!s) return true;
	const now = Date.now();
	if (s.lease_until > now) return false;
	const delay = s.next_due - now;
	if (delay > maxWait) return false;
	if (delay > 0) await new Promise((r) => setTimeout(r, Math.min(5e3, delay)));
	return s.next_due <= Date.now();
}
async function checkpoint() {
	writeJSON(paths.state, database.dump());
}
async function errorMap() {
	const errors = /* @__PURE__ */ new Map();
	const states = await database.prepare("SELECT id,last_error FROM collection_state WHERE last_error IS NOT NULL").all();
	for (const row of states.results) if (row.last_error) errors.set(row.id, row.last_error);
	const commerce = await database.prepare("SELECT payload FROM commerce_state WHERE id='uijeongbu'").first();
	const message = commerce ? JSON.parse(commerce.payload).error : null;
	if (message) errors.set("commerce", message);
	return errors;
}
async function reportNewErrors() {
	for (const [id, error] of await errorMap()) if (previousErrors.get(id) !== error) console.log("::warning::" + redact(`${id}: ${error}`).replace(/[\r\n]/g, " "));
}
var deadline = Date.now() + 18 * 6e4;
var remaining = () => deadline - Date.now();
async function runJob(name, fn) {
	if (remaining() < 9e4) {
		console.log(name + ": 다음 실행으로 이어갑니다.");
		return;
	}
	try {
		await fn();
	} catch (e) {
		console.log(name + ": " + redact(e instanceof Error ? e.message : "수집 지연 · 기존 자료 유지"));
	} finally {
		await checkpoint();
	}
}
async function runPopulation() {
	if (!await due("population")) return;
	const end = Math.min(Date.now() + 10 * 6e4, deadline - 9e4);
	for (let n = 0; n < 120 && Date.now() < end; n++) {
		const result = await collectPopulation(false);
		await checkpoint();
		if (!result.started || !result.pending || result.errors?.length) break;
		if ((await state("population"))?.last_error || !await due("population", 5e3)) break;
	}
}
async function runBirths() {
	for (let n = 0; n < 12 && remaining() > 9e4; n++) {
		const s = await state("births");
		if (n && s?.last_error || !await due("births", n ? 5e3 : 0)) break;
		await collectBirths(false);
		await checkpoint();
		const data = await loadBirths();
		if (data.error || !data.pending) break;
	}
}
async function runMonthly() {
	const pop = await readPopulation();
	const months = [...new Set([...pop?.population.map((x) => x.month) ?? [], ...recentMonths(/* @__PURE__ */ new Date(), 6)])].sort().reverse().slice(0, 6);
	for (const month of months) {
		if (remaining() < 9e4) break;
		for (const fn of [GET, GET$1]) {
			if (remaining() < 9e4) break;
			await (await fn(new Request(`https://collector.invalid/?month=${month}`))).json();
			await checkpoint();
		}
	}
}
async function runAreas() {
	const jobs = await database.prepare("SELECT j.id FROM population_jobs j JOIN collection_state s ON s.id=j.id WHERE j.id LIKE 'population-areas:%' AND json_extract(j.payload,'$.done')=0 AND s.next_due<=? AND s.lease_until<=? ORDER BY j.id LIMIT 8").bind(Date.now(), Date.now()).all();
	for (const row of jobs.results) {
		if (remaining() < 9e4) break;
		const [, month, id] = row.id.split(":");
		await collectPopulationAreas(id, month);
		await checkpoint();
	}
}
async function runCommerce() {
	const end = Math.min(Date.now() + 5 * 6e4, deadline - 9e4);
	for (let n = 0; n < 120 && Date.now() < end; n++) {
		if (n) {
			const data = await loadCommerce();
			if (!data.status.pending || data.status.error) break;
			if ((await readApiCache("commerce-retry-v2"))?.due > Date.now()) break;
		}
		const result = await collectCommerce(false);
		await checkpoint();
		if (!result.pending || result.error) break;
	}
}
async function collectNews() {
	if (!await due("news")) return;
	const started = Date.now(), old = readJSON(paths.news, {
		articles: [],
		collectedAt: null,
		lastAttemptAt: null,
		error: null
	});
	let next;
	try {
		const r = await fetch("https://news.google.com/rss/search?q=%EC%9D%98%EC%A0%95%EB%B6%80%EC%8B%9C&hl=ko&gl=KR&ceid=KR:ko", { signal: AbortSignal.timeout(25e3) });
		if (!r.ok) throw Error("Google RSS 응답 지연");
		const xml = await r.text();
		if (!/<(?:rss|feed)(?:\s|>)/i.test(xml)) throw Error("Google RSS 응답 형식 확인 필요");
		const incoming = parseNewsFeed(xml);
		if (!incoming.length) throw Error("Google RSS 기사 수신 대기");
		next = {
			articles: mergeNewsArticles(incoming, old.articles ?? []).slice(0, 150),
			collectedAt: nowISO(),
			lastAttemptAt: nowISO(),
			error: null,
			pending: false
		};
	} catch {
		next = {
			articles: (old.articles ?? []).slice(0, 150),
			collectedAt: old.collectedAt ?? null,
			lastAttemptAt: nowISO(),
			error: "Google RSS 수집 지연 · 마지막 정상 목록 유지",
			pending: false
		};
	}
	writeJSON(paths.news, next);
	await database.prepare("INSERT INTO collection_state(id,next_due,lease_until,last_started,last_finished,last_error) VALUES(?,?,0,?,?,?) ON CONFLICT(id) DO UPDATE SET next_due=excluded.next_due,lease_until=0,last_started=excluded.last_started,last_finished=excluded.last_finished,last_error=excluded.last_error").bind("news", Date.now() + HOUR, started, Date.now(), next.error).run();
}
async function buildExport() {
	const d = { ...original };
	const warnings = await readWarnings();
	if (warnings) d.warnings = {
		...warnings,
		pending: false,
		nextCheckAt: (await state("warnings"))?.next_due ?? null
	};
	const weather = await readSnapshot$1("city");
	if (weather) {
		const s = await state("weather");
		d.weather = {
			...weather,
			warnings: d.warnings ?? weather.warnings,
			grid: {
				...weather.grid,
				name: "의정부시"
			},
			collection: {
				pending: false,
				lastStarted: s?.last_started ? new Date(s.last_started).toISOString() : null,
				lastFinished: s?.last_finished ? new Date(s.last_finished).toISOString() : null,
				message: s?.last_error ?? null
			}
		};
	}
	const pop = await readPopulation();
	if (pop) d.population = pop;
	d.births = await loadBirths();
	d.commerce = await loadCommerce();
	d.traffic = await readTraffic();
	d.home = await readHomeSummary();
	d.monthly = { ...d.monthly ?? {} };
	const rows = await database.prepare("SELECT cache_key,payload FROM api_cache WHERE cache_key LIKE 'resident-age:v2:%' OR cache_key LIKE 'household-sizes:v1:%'").all();
	for (const r of rows.results) {
		const match = r.cache_key.match(/^(resident-age:v2|household-sizes:v1):(20\d{4})(:last-good)?$/);
		if (!match) continue;
		const v = JSON.parse(r.payload);
		if (!v.regions?.length) continue;
		const m = match[2], field = match[1].startsWith("resident") ? "ages" : "households";
		d.monthly[m] ??= {};
		const previous = d.monthly[m][field];
		if (!previous || !v.error || !previous.regions?.length) d.monthly[m][field] = v;
	}
	d.areas = { ...d.areas ?? {} };
	const areaJobs = await database.prepare("SELECT id FROM population_jobs WHERE id LIKE 'population-areas:%'").all();
	for (const row of areaJobs.results) {
		const [, m, id] = row.id.split(":");
		d.areas[`${m}:${id}`] = await loadPopulationAreas(id, m);
	}
	d.exportedAt = nowISO();
	if (!d.weather?.sources || !Array.isArray(d.population?.population) || !Array.isArray(d.traffic?.snapshots)) throw Error("JSON 내보내기 검증 실패 · 기존 파일 유지");
	return d;
}
async function exportData() {
	const d = await buildExport();
	writeJSON(paths.data, d);
	const actual = readJSON(paths.data);
	if (!actual.weather?.sources || !Array.isArray(actual.population?.population) || !Array.isArray(actual.traffic?.snapshots)) throw Error("JSON 저장 결과 검증 필요");
	console.log(JSON.stringify({
		savedAt: d.exportedAt,
		weatherSavedAt: d.weather?.storage?.savedAt,
		populationMonths: d.population?.population.map((m) => m.month),
		birthMonths: d.births.months.map((m) => m.month),
		commerceSavedAt: d.commerce.snapshot?.collectedAt,
		trafficYears: d.traffic.snapshots.map((m) => m.year),
		newsCount: readJSON(paths.news, { articles: [] }).articles.length
	}));
}
async function selfTest() {
	const x = new JsonDatabase();
	await x.prepare("INSERT INTO collection_state VALUES('test',9999999999999,9999999999999,1,NULL,NULL)").run();
	const y = new JsonDatabase();
	y.restore(x.dump());
	assert.equal((await y.prepare("SELECT * FROM collection_state WHERE id='test'").first()).next_due, 9999999999999);
	await assert.rejects(x.batch([x.prepare("INSERT INTO api_cache VALUES('x','{}',1)"), x.prepare("INSERT INTO api_cache VALUES('x','{}',2)")]));
	assert.equal(await x.prepare("SELECT * FROM api_cache WHERE cache_key='x'").first(), null);
	const fixture = readJSON(paths.data);
	assert(fixture?.weather?.sources);
	original = fixture;
	await initialSeed(fixture, readJSON(paths.news));
	let calls = 0;
	globalThis.fetch = async () => {
		calls++;
		throw Error("test provider unavailable");
	};
	await database.prepare("UPDATE collection_state SET next_due=0,lease_until=0,last_started=0 WHERE id='warnings'").run();
	await collectWarnings();
	const warningCalls = calls;
	await collectWarnings();
	assert.equal(calls, warningCalls);
	assert.equal((await readWarnings())?.issuedAt, fixture.warnings.issuedAt);
	const before = await readSnapshot$1("city");
	await database.prepare("UPDATE collection_state SET next_due=0,lease_until=0 WHERE id='weather'").run();
	await collectAndSave();
	assert.deepEqual((await readSnapshot$1("city"))?.current, before?.current);
	assert.equal((await database.prepare("SELECT region_id FROM weather_snapshots").all()).results.length, 1);
	await setInitialState("population", Date.now(), HOUR);
	assert.equal(await due("population", 5e3), false);
	const priorCalls = calls;
	await runPopulation();
	assert.equal(calls, priorCalls);
	await database.prepare("UPDATE collection_state SET next_due=0 WHERE id='population'").run();
	await collectPopulation(false);
	assert.equal(calls, priorCalls, "완료 인구 월은 재수집하지 않아야 합니다.");
	env.BIRTH_API_KEY = "test-key-only";
	await collectBirths(false);
	assert.equal(calls, priorCalls, "완료 출생 월은 재수집하지 않아야 합니다.");
	assert.equal((await loadBirths()).savedAt, fixture.births.savedAt);
	for (const month of Object.keys(fixture.monthly)) {
		await GET$1(new Request(`https://collector.invalid/?month=${month}`));
		await GET(new Request(`https://collector.invalid/?month=${month}`));
	}
	assert.equal(calls, priorCalls, "저장 월별 자료는 재사용해야 합니다.");
	await checkNewTraffic();
	assert.equal(calls, priorCalls, "완료 교통 연도는 강제 재수집하지 않아야 합니다.");
	const article = parseNewsFeed(`<rss version="2.0"><channel><item><title>의정부시 수집 검증</title><link>https://news.google.com/rss/articles/test</link><pubDate>${(/* @__PURE__ */ new Date()).toUTCString()}</pubDate><source>테스트</source></item></channel></rss>`);
	assert.equal(article.length, 1);
	assert.equal(mergeNewsArticles(article, article).length, 1);
	assert.doesNotThrow(() => JSON.parse(JSON.stringify(database.dump())));
	const exported = await buildExport();
	assert.equal(exported.weather.grid.name, "의정부시");
	assert.deepEqual(exported.weather.current, fixture.weather.current);
	assert.equal(exported.population.population[0].regions.length, 16);
	assert.equal(exported.traffic.snapshots.length, fixture.traffic.snapshots.length);
	assert.equal(exported.monthly[recentMonths()[0]].ages.regions.length, 16);
	console.log("검증 통과: JSON 상태 복원, 트랜잭션 복구, 특보 30분 제한, 시청 대표 날씨만 수집, 실패 시 기존 자료 유지, 미래 backoff 준수, 완료 인구·출생 월 재사용, 월별 자료 재사용, 교통 강제수집 방지, Google RSS 파싱·중복제거");
}
async function main() {
	if (process.argv.includes("--self-test")) {
		await selfTest();
		return;
	}
	if (!existsSync(paths.data)) throw Error("web/data.json이 없습니다. web 폴더부터 올려 주세요.");
	mkdirSync(dirname(paths.lock), { recursive: true });
	let lock;
	try {
		lock = openSync(paths.lock, "wx");
	} catch {
		console.log("다른 수집 작업이 진행 중이므로 중복 실행을 건너뜁니다.");
		return;
	}
	try {
		original = readJSON(paths.data);
		const saved = readJSON(paths.state);
		if (saved) database.restore(saved);
		else await initialSeed(original, readJSON(paths.news));
		previousErrors = await errorMap();
		const nativeFetch = globalThis.fetch.bind(globalThis);
		globalThis.fetch = (input, options = {}) => nativeFetch(input, {
			...options,
			signal: options.signal ? AbortSignal.any([options.signal, AbortSignal.timeout(9e4)]) : AbortSignal.timeout(9e4)
		});
		await runJob("warnings", async () => {
			const s = await state("warnings");
			if (await due("warnings") && (!s || Date.now() - s.last_started >= 30 * 6e4)) await collectWarnings();
		});
		await runJob("weather", async () => {
			if (await due("weather")) await collectAndSave();
		});
		await runJob("population", runPopulation);
		await runJob("births", runBirths);
		await runJob("monthly", runMonthly);
		await runJob("areas", runAreas);
		await runJob("commerce", runCommerce);
		await runJob("home", async () => {
			await checkNewTraffic();
			await refreshHomeSummary();
		});
		await runJob("news", collectNews);
		await exportData();
		await checkpoint();
		await reportNewErrors();
	} finally {
		closeSync(lock);
		unlinkSync(paths.lock);
	}
}
main().catch((e) => {
	console.error(redact(e instanceof Error ? e.message : "수집 실행 환경 확인 필요"));
	process.exitCode = 1;
});
//#endregion
export {};
