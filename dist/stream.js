"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.streamHandler = streamHandler;
const anixapi_1 = require("anixapi");
const utils_1 = require("./utils");
const resolve_1 = require("./resolve");
const client = new anixapi_1.Anixart({});
// None of the upstream fetches (Anixart API, Kodik, Sibnet, AniLibria) carry a timeout and the
// dubber walk used to be sequential, so one stalled source held the whole response well past
// Stremio's patience. Every step is bounded and dubbers are fanned out instead.
const STEP_TIMEOUT_MS = 8_000;
const RESPONSE_DEADLINE_MS = 20_000;
const CONCURRENCY = 8;
const MAX_STREAMS = 50;
// ANIXIO_DEBUG=1 logs where the time goes for each stream request (phase timings, timeouts).
const DEBUG = process.env.ANIXIO_DEBUG === "1";
const debug = (...parts) => { if (DEBUG)
    console.log("[stream]", ...parts); };
function withTimeout(promise, ms, fallback) {
    return new Promise((resolve) => {
        const timer = setTimeout(() => resolve(fallback), ms);
        promise.then((value) => { clearTimeout(timer); resolve(value); }, () => { clearTimeout(timer); resolve(fallback); });
    });
}
async function mapLimit(items, limit, fn) {
    let next = 0;
    const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
        while (next < items.length) {
            const index = next++;
            await fn(items[index], index);
        }
    });
    await Promise.all(workers);
}
function toDirectMp4(m3u8Url) {
    if (m3u8Url.includes(":hls:manifest.m3u8")) {
        return m3u8Url.replace(/:hls:manifest\.m3u8$/, "");
    }
    if (m3u8Url.endsWith(".m3u8")) {
        return m3u8Url.replace(/\.m3u8$/, ".mp4");
    }
    return null;
}
function fixUrl(url) {
    if (url.startsWith("//"))
        return `https:${url}`;
    return url;
}
// Anixart occasionally hands back the rest of an iframe tag after the URL, e.g.
// `https://video.sibnet.ru/shell.php?videoid=123" frameborder="0" scrolling="no`.
function cleanUrl(url) {
    return url.split(/["'\s]/)[0];
}
async function resolveStream(label, sourceUrl) {
    try {
        if (sourceUrl.includes("sibnet")) {
            const directUrl = await anixapi_1.SibnetParser.getDirectLink(sourceUrl);
            if (directUrl)
                return { name: "Anixart", title: label, url: fixUrl(directUrl) };
            return null;
        }
        if (sourceUrl.includes("kodik")) {
            const links = await anixapi_1.KodikParser.getDirectLinks(sourceUrl);
            if (!links)
                return null;
            for (const q of ["720", "480", "360", "240"]) {
                const sources = links[q];
                if (sources && sources.length > 0) {
                    const mp4Url = toDirectMp4(sources[0].src);
                    if (mp4Url) {
                        return { name: "Anixart", title: `${label} (${q}p)`, url: fixUrl(mp4Url) };
                    }
                }
            }
            return null;
        }
        if (sourceUrl.includes("libria") || sourceUrl.includes("anilibria")) {
            const links = await anixapi_1.AniLibriaParser.getDirectLinks(sourceUrl);
            if (!links)
                return null;
            for (const q of ["1080", "720", "480"]) {
                const source = links[q];
                if (source && source.src) {
                    const encoded = Buffer.from(JSON.stringify({ url: source.src, type: "libria" })).toString("base64url");
                    return {
                        name: "Anixart",
                        title: `${label} (${q}p) — CONSUMES SIGNIFICANT BANDWIDTH`,
                        url: `/play?m=${encoded}`,
                    };
                }
            }
            return null;
        }
        return null;
    }
    catch {
        return null;
    }
}
// Walks dubbers → sources → episodes and appends every matching episode to `out` as it is
// found, so a deadline hit mid-walk still leaves the caller with whatever was collected.
async function collectCandidates(releaseId, targetEpisode, out) {
    const typesResult = await withTimeout(client.endpoints.episode.types(releaseId), STEP_TIMEOUT_MS, null);
    if (!typesResult)
        debug(releaseId, "types: failed or timed out");
    const dubbers = typesResult?.types || [];
    await mapLimit(dubbers, CONCURRENCY, async (dubber, dubberIndex) => {
        const sourcesResult = await withTimeout(client.endpoints.episode.sources(releaseId, dubber.id), STEP_TIMEOUT_MS, null);
        if (!sourcesResult)
            debug(releaseId, dubber.name, "sources: failed or timed out");
        const sources = sourcesResult?.sources || [];
        for (const [sourceIndex, source] of sources.entries()) {
            const episodesResult = await withTimeout(client.endpoints.episode.episodes(releaseId, dubber.id, source.id), STEP_TIMEOUT_MS, null);
            if (!episodesResult)
                debug(releaseId, dubber.name, source.name, "episodes: failed or timed out");
            const episodes = episodesResult?.episodes || [];
            for (const ep of episodes) {
                if (targetEpisode !== null && ep.position !== targetEpisode)
                    continue;
                if (!ep.url)
                    continue;
                const baseLabel = `${dubber.name} | ${source.name}`;
                out.push({
                    order: dubberIndex * 10_000 + sourceIndex * 1_000 + ep.position,
                    dubberId: dubber.id,
                    label: ep.name ? `${baseLabel} — ${ep.name}` : baseLabel,
                    url: cleanUrl(ep.url),
                });
            }
        }
    });
}
async function streamHandler(args) {
    const started = Date.now();
    const remaining = () => Math.max(1_000, RESPONSE_DEADLINE_MS - (Date.now() - started));
    let episodeInfo = (0, utils_1.parseAnixartEpisodeId)(args.id);
    let releaseId = episodeInfo?.releaseId ?? (0, utils_1.parseAnixartId)(args.id);
    if (!releaseId) {
        const resolved = await withTimeout((0, resolve_1.resolveToAnixart)(args.type, args.id), remaining(), null);
        if (resolved) {
            releaseId = resolved;
            const parsed = args.id.match(/:(\d+):(\d+)$/);
            if (parsed) {
                episodeInfo = {
                    releaseId: resolved,
                    season: parseInt(parsed[1], 10),
                    episode: parseInt(parsed[2], 10),
                };
            }
        }
    }
    debug(args.id, "release", releaseId, "after", Date.now() - started, "ms");
    if (!releaseId)
        return { streams: [] };
    try {
        const targetEpisode = episodeInfo?.episode ?? null;
        const candidates = [];
        const collected = await withTimeout(collectCandidates(releaseId, targetEpisode, candidates).then(() => "done"), remaining(), "deadline");
        debug(args.id, "candidates", candidates.length, collected, "after", Date.now() - started, "ms");
        // Several dubbers (typically subtitle groups) point at the same player URL; resolve it once.
        const seen = new Set();
        const unique = candidates
            .sort((a, b) => a.order - b.order)
            .filter((c) => (seen.has(c.url) ? false : (seen.add(c.url), true)))
            .slice(0, MAX_STREAMS);
        const resolved = [];
        const finished = await withTimeout(mapLimit(unique, CONCURRENCY, async (c) => {
            const stream = await withTimeout(resolveStream(c.label, c.url), STEP_TIMEOUT_MS, null);
            if (stream) {
                stream.behaviorHints = { group: String(c.dubberId) };
                resolved.push({ order: c.order, stream });
            }
            else {
                debug(args.id, "no stream:", c.label, c.url.slice(0, 70));
            }
        }).then(() => "done"), remaining(), "deadline");
        debug(args.id, "streams", resolved.length, "of", unique.length, finished, "after", Date.now() - started, "ms");
        return { streams: resolved.sort((a, b) => a.order - b.order).map((r) => r.stream) };
    }
    catch (err) {
        console.error("Stream error:", err);
        return { streams: [] };
    }
}
