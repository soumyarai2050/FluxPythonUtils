import inspect
import ast
import os


def is_test_function(obj):
    """Check if an object is a test function."""
    return inspect.isfunction(obj) and obj.__name__.startswith("test")


def html_escape(text):
    """Escape HTML special characters."""
    return (text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


def format_docstring(docstring):
    """Format the docstring to preserve newlines as <br> in HTML."""
    if docstring:
        escaped = html_escape(docstring.strip())
        return escaped.replace("\n", "<br>")
    return "No documentation provided."


def get_functions_in_sequence(module_name):
    """Retrieve functions in their original sequence from the source file."""
    module = __import__(module_name)
    source_file = inspect.getsourcefile(module)
    with open(source_file, "r") as file:
        source_code = file.read()

    parsed = ast.parse(source_code)
    functions = [
        node.name for node in parsed.body
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test")
    ]
    return functions, module


def gen_test_html_doc(module_name):
    """Generate HTML documentation for test functions in original sequence."""
    function_names, module = get_functions_in_sequence(module_name)
    html_parts = [f"<html><body><h1>Test Documentation for {module_name}</h1>"]

    for func_name in function_names:
        obj = getattr(module, func_name)
        html_parts.append(f"<h2>{func_name}</h2><p>{format_docstring(obj.__doc__)}</p>")

    html_parts.append("</body></html>")
    return "\n".join(html_parts)


if __name__ == "__main__":
    # usage
    module_name = "test_file_name"  # Replace with your test script's name
    html_content = gen_test_html_doc(module_name)

    with open("test_functions_doc.html", "w") as f:
        f.write(html_content)
    print("HTML file generated: test_functions_doc.html")
