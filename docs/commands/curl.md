# Curl Command

The `curl` command runs the installed `curl` executable with arguments rendered from Bruin
connections. It is a pass-through wrapper, so it supports the complete option and variable
surface of the installed curl version without Bruin needing to duplicate curl's flags.

Put Bruin's flags before `--` and all curl flags, values, and URLs after it:

```bash
bruin curl -- \
  --request GET \
  --header 'Authorization: Bearer {{ bruin.connection("my-api").value }}' \
  "https://api.example.com/v1/items"
```

Bruin renders each argument and passes the resulting argument list directly to the installed curl
executable in the same order. It does not inspect, validate, or maintain a list of curl options, so
options supported by the installed curl version work without a corresponding Bruin release. Curl
alone interprets the rendered arguments and determines whether they are valid. Bruin connects
curl's stdin, stdout, and stderr directly to the caller.

## Referencing connections

Call `bruin.connection("<name>")` from Jinja to retrieve a connection by its existing Bruin name.
Its fields use the same snake_case names as `.bruin.yml`:

```yaml
environments:
  default:
    connections:
      generic:
        - name: my-api
          value: secret-api-token
        - name: analytics-api
          value: analytics-token
```

```bash
bruin curl -- \
  --header 'Authorization: Bearer {{ bruin.connection("my-api").value }}' \
  --header 'X-Analytics-Key: {{ bruin.connection("analytics-api").value }}' \
  'https://api.example.com/accounts/{{ bruin.connection("my-api").name }}'
```

One command may reference any number of connections, and repeated references to the same connection
are resolved only once. Fields nested inside a connection can be traversed with additional dots:

```jinja
{{ bruin.connection("service-api").credentials.client_id }}
```

The usual Bruin Jinja helpers remain available in the same namespace. For example,
<code v-pre>{{ bruin.slugify("Account Name") }}</code> renders as `account_name`. Most other helpers
generate SQL expressions and are primarily useful in asset templates; see [Jinja templating and
built-in functions](/assets/templating/macros).

Use `--environment` (or `--env`) to select a non-default environment and `--config-file` to use a
specific `.bruin.yml` file. Bruin's global `--secrets-backend` flag is also supported:

```bash
bruin --secrets-backend vault curl -- \
  --header 'Authorization: Bearer {{ bruin.connection("my-api").value }}' \
  https://api.example.com
```

Connections from a secrets backend are fetched by the names used in the Jinja expressions; Bruin
does not need to enumerate every secret in the backend.

## Curl variables

Curl uses <code v-pre>{{name}}</code>-style expressions for values supplied with `--variable`, which
overlaps with Jinja syntax. Bruin recognizes curl's variable syntax and leaves those expressions for
curl while still rendering connection references in the same argument:

```bash
bruin curl -- \
  --variable path=items \
  --expand-url 'https://{{ bruin.connection("my-api").host }}/v1/{{path:url}}'
```

Curl variable names are alphanumeric with underscores and do not contain dots, optionally followed
by colon-separated functions such as <code v-pre>{{path:trim:url}}</code>. Bruin connection expressions are
namespaced under `bruin.connection(...)`, which makes the two forms unambiguous.

All curl arguments, including option names, option values, URLs, and curl variable declarations,
are rendered independently. Use `bruin curl -- --help all` to view every option supported by the
installed curl executable.

> Rendered values are passed as curl process arguments and may be visible to local process-inspection
> tools. Curl can also reveal credentials through `--verbose`, `--trace`, `--trace-ascii`, `--libcurl`,
> and similar diagnostic output. Treat both the process arguments and diagnostic output as sensitive
> when a command references connection fields.
