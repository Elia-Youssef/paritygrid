import { CornerDownLeft, Search } from "lucide-react";
import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
} from "react";
import { useNavigate } from "react-router";

import { Button } from "./button";

export interface CommandDestination {
  to: string;
  label: string;
  description: string;
}

export interface CommandPaletteProps {
  destinations: readonly CommandDestination[];
  /** The key chord shown as the trigger hint; handled globally. */
  shortcutLabel?: string;
}

/**
 * Shell-level command palette. Opens with Ctrl+K (Cmd+K), keeps a single
 * combobox focus, and navigates with Enter. The dialog restores focus to
 * the trigger on dismissal; after navigating, the shell's route focus
 * management moves focus to the destination page heading.
 */
export function CommandPalette({
  destinations,
  shortcutLabel = "Ctrl K",
}: CommandPaletteProps) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const openRef = useRef(open);
  const navigate = useNavigate();

  const inputId = useId();
  const listboxId = useId();
  const optionId = useId();

  const filtered = useMemo(
    () => filterDestinations(destinations, query),
    [destinations, query],
  );
  const activeDestination = filtered[activeIndex];
  const activeOptionId =
    activeDestination === undefined ? undefined : `${optionId}-${activeIndex}`;

  const closePalette = useCallback(() => {
    setOpen(false);
    setQuery("");
    setActiveIndex(0);
    triggerRef.current?.focus();
  }, []);

  const goTo = useCallback(
    (destination: CommandDestination) => {
      closePalette();
      void navigate(destination.to);
    },
    [closePalette, navigate],
  );

  useEffect(() => {
    function handleShortcut(event: KeyboardEvent) {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        if (openRef.current) {
          closePalette();
        } else {
          setOpen(true);
        }
      }
    }
    window.addEventListener("keydown", handleShortcut);
    return () => window.removeEventListener("keydown", handleShortcut);
  }, [closePalette]);

  useEffect(() => {
    openRef.current = open;
  }, [open]);

  useEffect(() => {
    if (open) {
      inputRef.current?.focus();
    }
  }, [open]);

  // A new query invalidates the previous highlight. Resetting during
  // render keeps the derived selection consistent without an effect.
  const [queried, setQueried] = useState(query);
  if (queried !== query) {
    setQueried(query);
    setActiveIndex(0);
  }

  function handleInputKeyDown(event: ReactKeyboardEvent<HTMLInputElement>): void {
    const lastIndex = Math.max(filtered.length - 1, 0);
    switch (event.key) {
      case "Escape": {
        event.preventDefault();
        closePalette();
        break;
      }
      case "ArrowDown": {
        event.preventDefault();
        setActiveIndex((index) =>
          filtered.length === 0 ? 0 : (index + 1) % filtered.length,
        );
        break;
      }
      case "ArrowUp": {
        event.preventDefault();
        setActiveIndex((index) =>
          filtered.length === 0 ? 0 : (index - 1 + filtered.length) % filtered.length,
        );
        break;
      }
      case "Home": {
        event.preventDefault();
        setActiveIndex(0);
        break;
      }
      case "End": {
        event.preventDefault();
        setActiveIndex(lastIndex);
        break;
      }
      case "Enter": {
        event.preventDefault();
        if (activeDestination !== undefined) {
          goTo(activeDestination);
        }
        break;
      }
      case "Tab": {
        // The palette is a single-input dialog; keep focus on the combobox.
        event.preventDefault();
        break;
      }
      default:
        break;
    }
  }

  return (
    <>
      <Button
        ref={triggerRef}
        variant="ghost"
        size="compact"
        aria-label="Commands"
        aria-haspopup="dialog"
        aria-expanded={open}
        onClick={() => setOpen(true)}
        className="text-muted"
      >
        <Search className="size-3.5" aria-hidden="true" />
        {/* The label and chord hint collapse to an icon-only trigger on
            narrow screens; aria-label keeps the accessible name stable. */}
        <span className="hidden sm:inline">Commands</span>
        <span
          aria-hidden="true"
          className="ml-2 hidden items-center rounded border border-border px-1.5 py-0.5 font-mono text-2xs sm:inline-flex"
        >
          {shortcutLabel}
        </span>
      </Button>

      {open && (
        <div className="fixed inset-0 z-50 bg-background/70">
          <div
            role="dialog"
            aria-modal="true"
            aria-label="Command palette"
            className="mx-auto mt-[12vh] w-[calc(100%-2rem)] max-w-xl overflow-hidden rounded-lg border border-border-strong bg-surface shadow-overlay"
          >
            <div className="border-b border-border">
              <label htmlFor={inputId} className="sr-only">
                Search screens
              </label>
              <input
                id={inputId}
                ref={inputRef}
                type="text"
                role="combobox"
                aria-expanded="true"
                aria-controls={listboxId}
                aria-activedescendant={activeOptionId}
                aria-autocomplete="list"
                placeholder="Go to screen…"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                onKeyDown={handleInputKeyDown}
                className="h-12 w-full bg-transparent px-4 text-sm text-foreground placeholder:text-muted focus-visible:outline-none"
              />
            </div>
            <ul
              id={listboxId}
              role="listbox"
              aria-label="Destinations"
              className="max-h-80 overflow-y-auto p-1"
            >
              {filtered.map((destination, index) => (
                // Keyboard operation is owned by the combobox input above
                // through the ARIA activedescendant pattern; this pointer
                // handler is a mouse-only convenience.
                // eslint-disable-next-line jsx-a11y/click-events-have-key-events
                <li
                  key={destination.to}
                  id={`${optionId}-${index}`}
                  role="option"
                  aria-selected={index === activeIndex}
                  onMouseMove={() => setActiveIndex(index)}
                  onClick={() => goTo(destination)}
                  className={`flex cursor-pointer items-center justify-between gap-3 rounded-md px-3 py-2.5 ${
                    index === activeIndex ? "bg-surface-elevated" : ""
                  }`}
                >
                  {/* Keyboard selection is owned by the combobox input
                      above (Arrow keys plus Enter through the ARIA
                      activedescendant pattern); the pointer handlers are a
                      convenience for mouse users only. */}
                  <span className="min-w-0">
                    <span className="block text-sm font-medium text-foreground">
                      {destination.label}
                    </span>
                    <span className="block truncate text-xs text-muted">
                      {destination.description}
                    </span>
                  </span>
                  {index === activeIndex && (
                    <CornerDownLeft
                      className="size-3.5 shrink-0 text-muted"
                      aria-hidden="true"
                    />
                  )}
                </li>
              ))}
              {filtered.length === 0 && (
                <li
                  role="presentation"
                  className="px-3 py-6 text-center text-sm text-muted"
                >
                  No matching screen
                </li>
              )}
            </ul>
          </div>
        </div>
      )}
    </>
  );
}

function filterDestinations(
  destinations: readonly CommandDestination[],
  query: string,
): readonly CommandDestination[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery === "") {
    return destinations;
  }
  return destinations.filter(
    (destination) =>
      destination.label.toLowerCase().includes(normalizedQuery) ||
      destination.description.toLowerCase().includes(normalizedQuery),
  );
}
