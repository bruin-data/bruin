# Model contract: <name>

Question:     <the stakeholder question, verbatim>
Grain:        <what one row of the output represents>
Keys:         <the columns that uniquely identify a row>
Metric:       <the exact definition, including which column and which exclusions>
Filters:      <the non-negotiable inclusion rules>
Not included: <what this deliberately does not answer>

## Open decisions
<anything still unresolved, and who decides>

Copy this file to `docs/contracts/<model-name>.md` and fill every field before you
start writing the query. An unfilled field is a question for a stakeholder, not
something to guess at. Keep the contract short - if a field needs a paragraph, the
model probably needs to be split.
