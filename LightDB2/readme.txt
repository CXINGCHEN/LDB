Task 1: Detailed Logic for Extracting Join Conditions from the WHERE Clause
This process is designed within the following classes. 

Planner Class
In the Planner class, the system starts by parsing the SQL query to understand its structure, including the SELECT, FROM, and WHERE clauses. The part of extracting join conditions begins with the analysis of the WHERE clause.
The method first examines the FromItem and any additional joins specified in the query. This step is essential to understand the tables involved in the query and their relationships. The WHERE clause is then scrutinized to identify conditions that involve columns from more than one of the identified tables, distinguishing between simple selection conditions and conditions that necessitate a join operation.

JoinExpressionDeParser Class
For each expression in the WHERE clause, the class determines whether it is a join condition or a selection condition. This determination is based on whether the expression involves columns from multiple tables (join condition) or a single table (selection condition).
The core logic for identifying join conditions resides in the isJoin method, which checks if both sides of a comparison involve different tables. If only one side is a constant value or both sides refer to the same table, the condition is considered a selection condition for a single table.
The class overrides several visit methods (such as visit(EqualsTo equalsTo)), which are called for different types of comparison expressions encountered in the WHERE clause. These methods utilize the extractJoin function to decide whether the expression is a join condition and add it to the joinExpressionList if it is.

SelectExpressionDeParser Class
The SelectExpressionDeParser class is for evaluating whether tuples meet the specified join conditions. 
For every pair of tuples from the tables involved in the join, the SelectExpressionDeParser evaluates the join condition by considering the tuple data and the schema, ensuring that only tuples satisfying the join conditions are joined.

JoinOperator Class
The JoinOperator is where the join conditions are practically applied. It leverages the SelectExpressionDeParser to evaluate each pair of tuples from the left and right child operators against the join conditions.
the JoinOperator iterates through tuples from the left and right operators, applying the join condition to each pair. The SelectExpressionDeParser checks whether the combined tuple meets the join condition.
If a join condition exists and is satisfied, or if no condition is specified (implying a Cartesian product), the JoinOperator returns the combined tuple, effectively performing the join operation.


Task 2: Query Optimization Strategies in LightDB

Selection Pushdown
One of the primary optimization techniques used in LightDB is the pushdown of selection conditions. By applying selection conditions as close to the data source as possible, typically at the scan operators, the system ensures that only relevant tuples are passed up the query plan.The SelectOperator class is responsible for applying selection conditions directly to the tuples retrieved from the base tables. The SelectExpressionDeParser aids in this process by evaluating each tuple against the selection conditions.

Join Condition Optimization
The JoinOperator employs a nested loop approach for joins, iterating through tuples of the left and right operators. The SelectExpressionDeParser is used to evaluate the join conditions for each pair of tuples. By judiciously applying join conditions, the system avoids unnecessary Cartesian products, significantly reducing the size of intermediate results.

Early Projection
Applying projections early in the query plan helps in discarding unnecessary columns from tuples at an initial stage, reducing the tuple width and the amount of data that needs to be processed in subsequent operations.
The ProjectOperator is placed after base table operators or join operations to ensure that only the required columns are retained for further processing in the query plan.

Sort and Duplicate Elimination
The SortOperator is used judiciously, typically towards the end of the query plan, to ensure that sorting is done on as small a dataset as possible.The DuplicateEliminationOperator is applied after projections and sorting, further reducing the dataset size by removing duplicate tuples.

Materialization
In JoinOperator, materialization is employed make sure that for both left and right child, single table condition is processed before join.
