# Replace Run CQL Utility - Technical Documentation

This document details the internal technical structure, functions, flowcharts, and mindmaps of the standardized database query routing patcher (`replace_run_cql.py`).

## Technical Mindmap

```mermaid
mindmap
  root((replace_run_cql.py))
    Target Files List
      Applies to 13 key Python scripts
      Vali, Mipha, Spectrum, Catalyst, Spark, etc.
    Regex Replacement
      pattern checks: def run_cql_query
      re.subn with re.DOTALL flags
      Ensures code borders (def/class/main check) are respected
    Sanitization
      fixes joined definitions (e.g. strip()def -> strip()\\ndef)
      Enforces Unix line endings (newline=\\n)
```

## Function & Logic Breakdown

### Standardized Query Implementation
- Standardizes `run_cql_query` code block:
  - **Daruk Proxy route**: Performs HTTP POST to `http://127.0.0.1:9043/query`.
  - **cqlsh Fallback route**: Encodes CQL query as base64, pipes it into `podman exec -i systemd-hydra-db cqlsh <local_ip>` container executor.

### Regex Matching and Replacement Loop (`main()`)
- Walks the target script files list.
- Normalizes syntax borders: replaces `strip()def` or `strip()class` patterns.
- Applies regex search block:
  `pattern = r"def run_cql_query\b.*?(?=\n(?:def |class |if __name__)|\Z)"`
  to match only the `run_cql_query` function code definition.
- Writes the modified code block back to the target file with standard Unix newline formatting.
