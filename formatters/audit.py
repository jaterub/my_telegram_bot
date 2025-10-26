# formatters/audit.py
def fmt_summary(summary: dict, max_items: int = 5) -> str:
    def sec(title, key, emoji):
        blk = summary.get(key, {}) or {}
        cnt = blk.get("count", 0)
        items = (blk.get("items") or [])[:max_items]
        lines = [f"{emoji} *{title}*: *{cnt}*"]
        for it in items:
            if key == "unbalanced_tx":
                lines.append(
                    f"  • tx `{it.get('tx_id')}` — debit={it.get('sum_debit')} "
                    f"credit={it.get('sum_credit')} diff={it.get('diff')}  _{it.get('suggestion')}_"
                )
            else:
                lines.append(
                    f"  • fila {it.get('row')} tx `{it.get('tx_id')}` — {it.get('reason')}  _{it.get('suggestion')}_"
                )
        return "\n".join(lines)

    rows = summary.get("rows", 0)
    parts = [
        "📊 *Auditoría contable*",
        f"Total de filas: `{rows}`",
        sec("Fechas inválidas", "invalid_date", "🗓️"),
        sec("Duplicados (tx_id)", "duplicates_tx", "🔁"),
        sec("Desbalances", "unbalanced_tx", "⚖️"),
        sec("Obligatorios nulos", "required_nulls", "❗"),
    ]
    return "\n".join(parts)
