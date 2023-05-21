# finance-pipeline
Data pipeline to save personal finance transactions to a data warehouse for further analysis

The data input/control mechanism is a Google Sheets page that will be synced with the local DB on a daily basis (or when requested).

The following commands should be available:

- **init**: should receive two optional parameters: control sheets ID (`sid`) and/or the parent folder ID (`pid`).
    - if there is a `sid` and no `pid`, set the `pid` as the parent of `sid`.
    - if there is a `pid` and no `sid`, create a new control sheets inside the `pid`.
    - if there is no `sid` and no `sid`, create a `pid` without parents and a new control sheets inside it.
- **pull**: extracts data from control sheets and update the local db. It can receive an optional `year-month` as parameter.
    - if there is an `year-month`, ingests the corresponding sheets file to update local db.
    - as this is an already consolidated month, the statistics of each wallet should remain the same.
- **push**: create a sheets file for a specific `year-month` with local db data.
    - the process must also create a SQL file with a backup for the month in google drive.
- **close**: consolidate control sheets in current month's data and clear the control sheets.
    - the first step will be to compute the wallet's statistics and prompt the user for confirmation.
    - the process must sync the control sheets information with local db.
    - the process must create a sheets file in the `pid` with the consolidated month data.
    - the process must clear the control sheets (keeping recurrent expenses) and update the wallet's statistics.
- **extract**: extract's data from local db and create a CSV file.
    - should receive optional filters, e.g., wallet, currency, period, category, event, place, etc.
