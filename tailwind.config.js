/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./app/**/*.{js,jsx}", "./components/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#0b1020",
        panel: "#121933",
        panel2: "#1a2247",
        accent: "#7aa2ff",
        accent2: "#f7b955",
        good: "#5ad48b",
        bad: "#ff7a90",
      },
      fontFamily: {
        sans: ["Inter", "ui-sans-serif", "system-ui"],
      },
    },
  },
  plugins: [],
};
