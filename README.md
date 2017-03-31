# Servus - Salesforce data connector that uses Bulk API.

## Structure

Main file is src/clj/servus/engines.clj, it contains definition of the workflow, everything else is
support/framework code to make it work.

## How to start

Copy profiles.clj.example to profiles.clj.

Start REPL

    lein repl

Invoke sample run of fetching data

    (servus.core/login "user@dev-ed-sf-org.com" "your-password")

Load changes (and restart state) after changing code

    (reset)
