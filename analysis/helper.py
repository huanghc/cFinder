import gast as ast, ast as old_ast
from util import util
import logging, sys

if sys.version_info.major >= 3:
    from importlib import reload

reload(util)


def read_file(filename):
    with open(filename) as fd:
        return fd.read()


#
# Filepath: full path of a file
# @return
#
def get_module_from_path(filepath):
    try:
        source = read_file(filepath)
    except Exception as e:
        logging.error(e)

    module = ast.parse(source)
    return module, source


class KeywordNodesFromSubTree(ast.NodeVisitor):
    def __init__(self):
        self.visited_nodes = []
        self.nodes = []

    def visit_keyword(self, node):
        if node.arg != "defaults" and node.arg:
            self.visited_nodes.append(node.arg)
            self.nodes.append(node)

        self.generic_visit(node)


#
# Get the list of column = val on the right hand side of the table.get(col1=, col2=)
#
def get_column_lists(node):
    attr = KeywordNodesFromSubTree()
    attr.visit(node)
    return attr.visited_nodes


#
# Check if the pk exists in the arg list. If so, return the value node.
#
def get_column_list_with_node_for_fk(node, pk_name):
    attr = KeywordNodesFromSubTree()
    attr.visit(node)
    for idx, keyword in enumerate(attr.visited_nodes):
        if pk_name == keyword:
            return keyword, attr.nodes[idx].value

    return False, None
