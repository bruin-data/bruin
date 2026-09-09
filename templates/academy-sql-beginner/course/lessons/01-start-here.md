# Lesson 01: start-here

## Objectives
- Define table, row, query, and warehouse in one sentence each.
- Say how an agent differs from a chatbot, and why that difference is the whole course.

## Concepts to teach
This course is about reading and checking SQL, not writing it fast. Before any of
that, five words. A **table** is a grid of data: columns are the fields, rows are the
records. A **row** is one record - one order, one customer. A **query** is a question
you ask the data in SQL, and the answer comes back as a table. A **warehouse** is a
database built for asking those questions; here it is a single local DuckDB file, no
account and no server.

The fifth word is why you are doing this with an agent. A **chatbot** writes text back
to you. An **agent** can run commands and read files in this project - it can actually
run a query, read the result, and act on it. That power is the point and the risk: an
agent can hand you a number that is confidently, quietly wrong, and it will sound just
as sure as when it is right. Your job by the end is to catch that.

## Quiz
1. Q: What is the difference between a table's columns and its rows?
   A: Columns are the fields (order_id, order_total); a row is one record with a value in each field.
2. Q: An agent runs a query and tells you "revenue was 1.4 million." How is that different from a chatbot saying the same thing?
   A: The agent actually ran SQL against the data, so the number is real output - but the query behind it can still be wrong. A chatbot is only predicting text. Either can be wrong; only the agent's answer looks like proof.

## Task
Answer these four checkpoint questions in your own words (say them out loud or write
them down): what is a table, what is a row, what is a query, what is a warehouse? Then
answer one more: how is an agent different from a chatbot?

## Rubric (for `review my work`)
- [ ] Table, row, query, and warehouse are each defined correctly in plain language.
- [ ] The student distinguishes an agent from a chatbot: an agent runs commands and reads real results, a chatbot only returns text - and a running query can still be wrong.

## Done signal
Confirm the student can name all five terms and can say why an agent's answer can be wrong without looking wrong. Carry forward: this whole course is checking numbers, not producing them.
