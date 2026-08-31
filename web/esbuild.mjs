import * as esbuild from "esbuild";
import { cp, mkdir } from "node:fs/promises";

const out = "../internal/webui/dist";
await mkdir(out, { recursive: true });
await esbuild.build({
  entryPoints: ["src/app.ts"],
  bundle: true,
  minify: true,
  sourcemap: false,
  outdir: out,
  target: ["es2022"],
});
await cp("src/index.html", `${out}/index.html`);
