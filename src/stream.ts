import { Anixart, KodikParser, AniLibriaParser, SibnetParser } from "anixapi";
import type { ContentType, Stream } from "stremio-addon-sdk";
import { parseAnixartId, parseAnixartEpisodeId } from "./utils";
import { resolveToAnixart } from "./resolve";

const client = new Anixart({});

// None of the upstream fetches (Anixart API, Kodik, Sibnet, AniLibria) carry a timeout and the
// dubber walk used to be sequential, so one stalled source held the whole response well past
// Stremio's patience. Every step is bounded and dubbers are fanned out instead.
const STEP_TIMEOUT_MS = 8_000;
const RESPONSE_DEADLINE_MS = 20_000;
const CONCURRENCY = 8;
const MAX_STREAMS = 50;

// ANIXIO_DEBUG=1 logs where the time goes for each stream request (phase timings, timeouts).
const DEBUG = process.env.ANIXIO_DEBUG === "1";
const debug = (...parts: unknown[]) => { if (DEBUG) console.log("[stream]", ...parts); };

interface Candidate {
  order: number;
  dubberId: number;
  label: string;
  url: string;
}

function withTimeout<T>(promise: Promise<T>, ms: number, fallback: T): Promise<T> {
  return new Promise<T>((resolve) => {
    const timer = setTimeout(() => resolve(fallback), ms);
    promise.then(
      (value) => { clearTimeout(timer); resolve(value); },
      () => { clearTimeout(timer); resolve(fallback); },
    );
  });
}

async function mapLimit<T>(items: T[], limit: number, fn: (item: T, index: number) => Promise<void>): Promise<void> {
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const index = next++;
      await fn(items[index], index);
    }
  });
  await Promise.all(workers);
}

function toDirectMp4(m3u8Url: string): string | null {
  if (m3u8Url.includes(":hls:manifest.m3u8")) {
    return m3u8Url.replace(/:hls:manifest\.m3u8$/, "");
  }
  if (m3u8Url.endsWith(".m3u8")) {
    return m3u8Url.replace(/\.m3u8$/, ".mp4");
  }
  return null;
}

function fixUrl(url: string): string {
  if (url.startsWith("//")) return `https:${url}`;
  return url;
}

// Anixart occasionally hands back the rest of an iframe tag after the URL, e.g.
// `https://video.sibnet.ru/shell.php?videoid=123" frameborder="0" scrolling="no`.
function cleanUrl(url: string): string {
  return url.split(/["'\s]/)[0];
}

async function resolveStream(label: string, sourceUrl: string): Promise<Stream | null> {
  try {
    if (sourceUrl.includes("sibnet")) {
      const directUrl = await SibnetParser.getDirectLink(sourceUrl);
      if (directUrl) return { name: "Anixart", title: label, url: fixUrl(directUrl) };
      return null;
    }

    if (sourceUrl.includes("kodik")) {
      const links = await KodikParser.getDirectLinks(sourceUrl);
      if (!links) return null;

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
      const links = await AniLibriaParser.getDirectLinks(sourceUrl);
      if (!links) return null;

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
  } catch {
    return null;
  }
}

// Walks dubbers → sources → episodes and appends every matching episode to `out` as it is
// found, so a deadline hit mid-walk still leaves the caller with whatever was collected.
async function collectCandidates(releaseId: number, targetEpisode: number | null, out: Candidate[]): Promise<void> {
  const typesResult = await withTimeout(client.endpoints.episode.types(releaseId), STEP_TIMEOUT_MS, null);
  if (!typesResult) debug(releaseId, "types: failed or timed out");
  const dubbers = typesResult?.types || [];

  await mapLimit(dubbers, CONCURRENCY, async (dubber, dubberIndex) => {
    const sourcesResult = await withTimeout(client.endpoints.episode.sources(releaseId, dubber.id), STEP_TIMEOUT_MS, null);
    if (!sourcesResult) debug(releaseId, dubber.name, "sources: failed or timed out");
    const sources = sourcesResult?.sources || [];

    for (const [sourceIndex, source] of sources.entries()) {
      const episodesResult = await withTimeout(
        client.endpoints.episode.episodes(releaseId, dubber.id, source.id),
        STEP_TIMEOUT_MS,
        null,
      );
      if (!episodesResult) debug(releaseId, dubber.name, source.name, "episodes: failed or timed out");
      const episodes = episodesResult?.episodes || [];

      for (const ep of episodes) {
        if (targetEpisode !== null && ep.position !== targetEpisode) continue;
        if (!ep.url) continue;

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

export async function streamHandler(args: { type: ContentType; id: string }): Promise<{ streams: Stream[] }> {
  const started = Date.now();
  const remaining = () => Math.max(1_000, RESPONSE_DEADLINE_MS - (Date.now() - started));

  let episodeInfo = parseAnixartEpisodeId(args.id);
  let releaseId = episodeInfo?.releaseId ?? parseAnixartId(args.id);

  if (!releaseId) {
    const resolved = await withTimeout(resolveToAnixart(args.type, args.id), remaining(), null);
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
  if (!releaseId) return { streams: [] };

  try {
    const targetEpisode = episodeInfo?.episode ?? null;

    const candidates: Candidate[] = [];
    const collected = await withTimeout<string>(
      collectCandidates(releaseId, targetEpisode, candidates).then(() => "done"),
      remaining(),
      "deadline",
    );
    debug(args.id, "candidates", candidates.length, collected, "after", Date.now() - started, "ms");

    // Several dubbers (typically subtitle groups) point at the same player URL; resolve it once.
    const seen = new Set<string>();
    const unique = candidates
      .sort((a, b) => a.order - b.order)
      .filter((c) => (seen.has(c.url) ? false : (seen.add(c.url), true)))
      .slice(0, MAX_STREAMS);

    const resolved: Array<{ order: number; stream: Stream }> = [];
    const finished = await withTimeout<string>(
      mapLimit(unique, CONCURRENCY, async (c) => {
        const stream = await withTimeout(resolveStream(c.label, c.url), STEP_TIMEOUT_MS, null);
        if (stream) {
          stream.behaviorHints = { group: String(c.dubberId) };
          resolved.push({ order: c.order, stream });
        } else {
          debug(args.id, "no stream:", c.label, c.url.slice(0, 70));
        }
      }).then(() => "done"),
      remaining(),
      "deadline",
    );
    debug(args.id, "streams", resolved.length, "of", unique.length, finished, "after", Date.now() - started, "ms");

    return { streams: resolved.sort((a, b) => a.order - b.order).map((r) => r.stream) };
  } catch (err) {
    console.error("Stream error:", err);
    return { streams: [] };
  }
}
