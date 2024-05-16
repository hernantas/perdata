import ts from '@rollup/plugin-typescript'
import { sync } from 'glob'
import { extname, relative } from 'node:path'
import { rollup } from 'rollup'

async function build() {
  const files = Object.fromEntries(
    sync('./src/**/*.ts', {
      ignore: ['./src/**/*.test.ts'],
    }).map((file) => [
      relative('src', file.slice(0, file.length - extname(file).length)),
      file,
    ]),
  )
  const bundle = await rollup({
    input: files,
    plugins: [ts({ outDir: './dist', exclude: './**/*.test.ts' })],
  })

  await bundle.write({
    dir: './dist',
    format: 'esm',
    entryFileNames: '[name].mjs',
    sourcemap: true,
  })

  await bundle.write({
    dir: './dist',
    format: 'cjs',
    entryFileNames: '[name].cjs',
    sourcemap: true,
  })
}

build()
