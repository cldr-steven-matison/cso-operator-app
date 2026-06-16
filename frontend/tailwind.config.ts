import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#0b0d10",
        panel: "#13161b",
        border: "#1f242b",
        muted: "#7d8794",
        text: "#e6edf3",
        accent: "#3fb950",
        warn: "#d29922",
        bad: "#f85149",
      },
    },
  },
  plugins: [],
} satisfies Config;
