
# Automated unit and regression testing 
Great Videos to start: 
1. https://youtu.be/UMgxJvozR5A
2. https://youtu.be/DhUpxWjOhME

## Principal - Test Driven Development - gives a framework to know your code is behaving as expected
1. write skeleton of the entry/primary/key function/class you intend to write
   1. Write it with well thought Name, Parameters, Return Type, and dummy return value
2. Plan test case:
   1. Think of use-cases the function is to serve  
   2. Identify what parameters user should pass for each of these use-cases
   3. Define what caller is to expect as return for "each use-case" invocation
   4. Think of edge or boundary case: 
      1. these are valid but unusual arguments caller could pass 
      2. think what should caller expect in return on calls with these unusual arguments
3. write the planned test cases
   1. Test functions make all use-cases and edge-cases calls with required inputs
   2. it then compares expected return value with actual return value via asserts
4. Execute the written test case
   1. Most Tests would fail to begin with (as skeleton code just returns  dummy value)
   2. Now implement functional logic in the skeleton functions, leading to fixing of failing test cases 
   3. Ensuring all test case passes once desired functionality is correctly implemented
5. repeat 2, 3, 4 until function meets the specification and desired behaviour 
6. Testing is not a substitute for critical thinking - think to ensure you have covered required grounds
7. writing more than required tests and less than required tests are both bad - test case per use/edge-case
8. if a function has if else - write test case that reaches each branch
## PyTest - Test Tool of choice
Installation:
`pip install -U pytest`
`pip install pytest-html`
`pip install pytest-cov`
Test Execution: (assuming tests are in tests dir)
`pytest --cov=myproj tests/`
Test Report Generation:
`pytest --html=report.html --self-contained-html`
Needs further experiment:
`pip install pytest-localserver`
