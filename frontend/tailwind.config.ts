import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          blue: '#0576F3',
          green: '#36F48C',
          orange: '#F47806',
          yellow: '#F3F40B',
          pink: '#F479F4',
        },
      },
      fontFamily: {
        ballinger: ['var(--font-ballinger)', 'monospace'],
      },
    },
  },
  plugins: [],
}

export default config
