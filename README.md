# JSON-filter with MongoDB shell syntax

## Command line JSON filter inspired mongo shell commands syntax

### How to install

    pip3 install json-filter

### How to use

#### Write your filter inline

    wget -qO - "http://rest.best.com/data" | jf -f '{"jf.tool": true}'

#### Read filters from file

    cat examples/data.json  | jf -ff examples/filter.json

#### Or combine both

    wget -qO - "http://rest.best.com/data" | jf -ff /path/to/filters.json -f "{"errors": {"$size": 2}}"

### Available expressions

- $gt: matches values that are greater than a specified value;
- $gte: matches values that are greater than or equal to a specified value;
- $eq: matches values that are equal to a specified value;
- $lt: matches values that are less than a specified value;
- $lte: matches values that are less than or equal to a specified value;
- $ne: matches all values that are not equal to a specified value;
- $in: matches any of the values specified in an array;
- $nin: matches none of the values specified in an array;
- $exists: matches documents that have the specified field;
- $regex: selects documents where values match a specified regular expression;
- $size: selects documents if the array field is a specified size;
- $and: joins query clauses with a logical AND returns all documents that match the conditions of both clauses;
- $or: joins query clauses with a logical OR returns all documents that match the conditions of either clause;
- $nor: joins query clauses with a logical NOR returns all documents that fail to match both clauses; 
- $not: inverts the effect of a query expression and returns documents that do not match the query expression.
- $elemMatch: matches documents that contain an array field with at least one element that matches all the specified query critery

### Filter example 

    {
        "field1": "value1",
        "field2": {
            "$gt": "field2_value"
        },
        "field3": {
            "$or": [
                {
                    "name1": "value1"
                },
                {
                    "name2": {
                        "$in": [
                            "val1",
                            "val2"
                        ]
                    }
                }
            ]
        }
    }

### Colorize output (optional)

    python3 -m pip install Pygments
    cat examples/data.json  | jf -ff examples/filter.json -c
