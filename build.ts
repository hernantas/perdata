import { rollup } from 'rollup'
import ts from '@rollup/plugin-typescript'

async function build() {
  const bundle = await rollup({
    input: './src/index.ts',
    plugins: [ts({ outDir: './dist', exclude: './**/*.test.ts' })],
  })

  await bundle.write({
    dir: './dist',
    format: 'esm',
    entryFileNames: '[name].mjs',
    preserveModules: true,
    sourcemap: true,
  })

  await bundle.write({
    dir: './dist',
    format: 'cjs',
    entryFileNames: '[name].cjs',
    preserveModules: true,
    sourcemap: true,
  })
}

build()
