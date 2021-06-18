# urban-happiness

This node script parses data for TripleLift.


## Prerequisites: 
- Node.js v14.11.0 (or later)
- A working internet connection (hah!)


## Installation

### Step 1:

Clone the repository and CD into it.

```
git clone https://github.com/foreza/urban-happiness.git
cd urban-happiness/
```

### Step 2: 

Run `npm i` to install all dependencies.

You're all set. 


## Usage

`parser.js` will take a CSV file as input.

### Test Run on a smaller data set:

```js
node parser.js sample_data/tactic-small.csv
```


### Actual run with a larger data set

```js
node parser.js sample_data/tactic.csv`
```


### Output:

You'll get a CSV with any URLs that failed in the data set.

Consult the `sample_data` directory for examples.

Output file name will be in the project root directory Example:
`failed-url-results-1624042470603.csv`


## TODO:
- Revamp how requests are batched and sent.