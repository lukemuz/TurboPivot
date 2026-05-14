import { useMemo } from "react";
import {
  DndContext,
  DragEndEvent,
  PointerSensor,
  useDraggable,
  useDroppable,
  useSensor,
  useSensors,
} from "@dnd-kit/core";
import { AggregationType, ColumnInfo, ColumnDtype, ValueWithAggregation } from "./types";

export type ShelfId = "available" | "rows" | "columns" | "values";

export interface ShelfState {
  rows: string[];
  columns: string[];
  values: ValueWithAggregation[];
}

interface ShelvesProps {
  columns: ColumnInfo[];
  state: ShelfState;
  onChange: (next: ShelfState) => void;
}

function dtypeBadge(dtype: ColumnDtype): string {
  switch (dtype) {
    case ColumnDtype.String:
      return "abc";
    case ColumnDtype.Integer:
      return "123";
    case ColumnDtype.Float:
      return "1.2";
    case ColumnDtype.Boolean:
      return "T/F";
    case ColumnDtype.Date:
      return "📅";
    case ColumnDtype.Datetime:
      return "🕒";
    default:
      return "?";
  }
}

function FieldChip({
  id,
  label,
  badge,
  onRemove,
  children,
}: {
  id: string;
  label: string;
  badge?: string;
  onRemove?: () => void;
  children?: React.ReactNode;
}) {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({ id });
  return (
    <div
      ref={setNodeRef}
      className="field-chip"
      style={{
        opacity: isDragging ? 0.5 : 1,
        cursor: "grab",
        display: "inline-flex",
        alignItems: "center",
        gap: "0.4em",
        padding: "0.25em 0.5em",
        margin: "0.15em",
        background: "#f0f4f8",
        border: "1px solid #cbd5e0",
        borderRadius: "4px",
        fontSize: "0.9em",
      }}
      {...attributes}
      {...listeners}
    >
      {badge && (
        <span
          style={{
            fontSize: "0.7em",
            color: "#718096",
            background: "#e2e8f0",
            padding: "0 0.3em",
            borderRadius: "2px",
          }}
        >
          {badge}
        </span>
      )}
      <span>{label}</span>
      {children}
      {onRemove && (
        <button
          type="button"
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => {
            e.stopPropagation();
            onRemove();
          }}
          style={{
            border: "none",
            background: "transparent",
            cursor: "pointer",
            padding: "0 0.2em",
            color: "#718096",
            fontSize: "0.9em",
          }}
          title="Remove"
        >
          ×
        </button>
      )}
    </div>
  );
}

function Shelf({
  id,
  label,
  hint,
  children,
}: {
  id: ShelfId;
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  const { setNodeRef, isOver } = useDroppable({ id });
  return (
    <div
      ref={setNodeRef}
      style={{
        border: `2px dashed ${isOver ? "#4a90e2" : "#cbd5e0"}`,
        background: isOver ? "#ebf8ff" : "#fafafa",
        padding: "0.5em",
        borderRadius: "6px",
        minHeight: "3em",
        marginBottom: "0.5em",
      }}
    >
      <div style={{ fontSize: "0.8em", fontWeight: 600, color: "#4a5568", marginBottom: "0.3em" }}>
        {label}
      </div>
      <div style={{ minHeight: "2em" }}>{children}</div>
      {hint && (
        <div style={{ fontSize: "0.75em", color: "#a0aec0", marginTop: "0.3em" }}>{hint}</div>
      )}
    </div>
  );
}

function fieldShelf(state: ShelfState, field: string): ShelfId {
  if (state.rows.includes(field)) return "rows";
  if (state.columns.includes(field)) return "columns";
  if (state.values.some((v) => v.field === field)) return "values";
  return "available";
}

function removeFromAll(state: ShelfState, field: string): ShelfState {
  return {
    rows: state.rows.filter((f) => f !== field),
    columns: state.columns.filter((f) => f !== field),
    values: state.values.filter((v) => v.field !== field),
  };
}

export default function Shelves({ columns, state, onChange }: ShelvesProps) {
  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 4 } }));

  const placed = useMemo(() => {
    const set = new Set<string>();
    state.rows.forEach((f) => set.add(f));
    state.columns.forEach((f) => set.add(f));
    state.values.forEach((v) => set.add(v.field));
    return set;
  }, [state]);

  const available = columns.filter((c) => !placed.has(c.name));
  const columnInfoByName: Record<string, ColumnInfo> = useMemo(
    () => Object.fromEntries(columns.map((c) => [c.name, c])),
    [columns]
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over) return;
    const field = String(active.id);
    const targetShelf = String(over.id) as ShelfId;
    if (targetShelf === fieldShelf(state, field)) return;

    let next = removeFromAll(state, field);
    if (targetShelf === "rows") {
      next.rows = [...next.rows, field];
    } else if (targetShelf === "columns") {
      next.columns = [...next.columns, field];
    } else if (targetShelf === "values") {
      next.values = [...next.values, { field, aggregation: defaultAgg(columnInfoByName[field]) }];
    }
    onChange(next);
  };

  const updateAgg = (field: string, agg: AggregationType) => {
    onChange({
      ...state,
      values: state.values.map((v) => (v.field === field ? { ...v, aggregation: agg } : v)),
    });
  };

  return (
    <DndContext sensors={sensors} onDragEnd={handleDragEnd}>
      <Shelf id="rows" label="Rows" hint="Fields here become row headers">
        {state.rows.map((f) => (
          <FieldChip
            key={f}
            id={f}
            label={f}
            badge={columnInfoByName[f] && dtypeBadge(columnInfoByName[f].dtype)}
            onRemove={() => onChange(removeFromAll(state, f))}
          />
        ))}
      </Shelf>
      <Shelf id="columns" label="Columns" hint="Fields here pivot into columns">
        {state.columns.map((f) => (
          <FieldChip
            key={f}
            id={f}
            label={f}
            badge={columnInfoByName[f] && dtypeBadge(columnInfoByName[f].dtype)}
            onRemove={() => onChange(removeFromAll(state, f))}
          />
        ))}
      </Shelf>
      <Shelf id="values" label="Values" hint="Fields here are aggregated">
        {state.values.map((v) => (
          <FieldChip
            key={v.field}
            id={v.field}
            label={v.field}
            badge={columnInfoByName[v.field] && dtypeBadge(columnInfoByName[v.field].dtype)}
            onRemove={() => onChange(removeFromAll(state, v.field))}
          >
            <select
              value={v.aggregation}
              onPointerDown={(e) => e.stopPropagation()}
              onClick={(e) => e.stopPropagation()}
              onChange={(e) => updateAgg(v.field, e.target.value as AggregationType)}
              style={{ marginLeft: "0.3em", fontSize: "0.85em" }}
            >
              {Object.values(AggregationType).map((a) => (
                <option key={a} value={a}>
                  {a}
                </option>
              ))}
            </select>
          </FieldChip>
        ))}
      </Shelf>
      <Shelf id="available" label="Available Fields" hint="Drag onto a shelf to use">
        {available.map((c) => (
          <FieldChip key={c.name} id={c.name} label={c.name} badge={dtypeBadge(c.dtype)} />
        ))}
      </Shelf>
    </DndContext>
  );
}

function defaultAgg(info: ColumnInfo | undefined): AggregationType {
  if (!info) return AggregationType.Sum;
  if (info.dtype === ColumnDtype.Integer || info.dtype === ColumnDtype.Float) {
    return AggregationType.Sum;
  }
  return AggregationType.Count;
}
