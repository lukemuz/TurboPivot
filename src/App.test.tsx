import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import App from "./App";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: vi.fn(),
  save: vi.fn(),
}));
vi.mock("@tauri-apps/plugin-fs", () => ({
  writeTextFile: vi.fn(),
  readTextFile: vi.fn(),
}));

describe("App", () => {
  it("renders the app title", () => {
    render(<App />);
    expect(screen.getByText("TurboPivot")).toBeInTheDocument();
  });

  it("renders the tagline", () => {
    render(<App />);
    expect(
      screen.getByText(/Lightning fast pivot tables powered by Rust & Polars/i)
    ).toBeInTheDocument();
  });

  it("shows the data source picker", () => {
    render(<App />);
    expect(screen.getByText("Data Source")).toBeInTheDocument();
  });

  it("renders the empty pivot-table state before any source is loaded", () => {
    render(<App />);
    expect(
      screen.getByText(/Configure and run a pivot to see results here/i)
    ).toBeInTheDocument();
  });

  it("offers File, SQLite and Postgres source modes", () => {
    render(<App />);
    expect(screen.getByRole("button", { name: "File" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "SQLite" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Postgres" })).toBeInTheDocument();
  });
});
