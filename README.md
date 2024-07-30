# Mini-DBMS

A very simplified Database Management System which simulates a real DBMS using Java serialization.

## Supported Functionalities

- **Creating Tables**
- **Inserting / Updating Tables**
- **Deleting Tuples**
- **Linear Search Through the Table**
- **Creating a B+ Tree Index on a Specific Column**
- **Using Any Created B+ Tree Index Where Appropriate**

## Unsupported Functionalities

- No Foreign Keys
- No Referential Integrity Constraints
- No Joins

## TO-DO

- Parsing Real SQL via ANTLR

## Notices

- The number of rows per page, which defaults to 200, can be changed in the [DBApp.config](https://github.com/grython7/Mini-DBMS/blob/main/src/main/resources/DBApp.config) file.
- On page overflow, the page is split into half to form two pages.
