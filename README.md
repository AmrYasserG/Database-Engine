# Database Engine

### Implemented in java; supporting CRUD operations, selection and indexing.</br>
This project attempts to simulate how an actual database engine works, through storing tuples/rows inside tables into **serialized pages** (stored as sequences of bits)
on the disk,
which can then be accessed by the project's methods, **deserialized** and used.</br>

The project automatically sorts all inserted tuples into the tables on a pre-determined clustering key (key used to sort the table) by the user. Insertion
supports the creation of overflow pages for when a page is full, and automatically shifts and repositions tuples inside the pages so that they are accurately sorted.</br>

Indexing is also supported using an **N-dimensional Grid Index**, as well as selecting certain tuples either from the grid index or from the deserialized pages themselves.
