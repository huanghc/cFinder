import gast as ast, ast as old_ast
import logging, sys
from . import helper
from . import basic_traverse

if sys.version_info.major >= 3:
    from importlib import reload
reload(helper)
reload(basic_traverse)


#######################
## Get Keyword pattern for constraints. Such as:
# Model.objects.get_or_create(column = , )
#######################
class UniqueFinderGet(basic_traverse.AttrAnalysis):

    GET_KEYWORDS = ["get", "get_or_create", "update_or_create"]
    GET_KEYWORDS_ID = ["get_object_or_404", "get_object_for_this_type"]

    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(UniqueFinderGet, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "unique"

    #
    # Call nodes for pattern1: attr_list.get(col)
    #
    def visit_Call(self, node):
        # Clean it every time. Or the provious will polute it.
        self.cols = []
        self.extra = ""

        if node in self.visited_nodes:
            return

        self.visited_nodes.append(node)

        get_type = self._is_get(node)
        if get_type:
            # If keyword is pk or id, no need to record this.
            if node.keywords and (
                not node.keywords[0]
                or (node.keywords[0].arg == "pk")
                or (node.keywords[0].arg == "id")
            ):
                return

            # Filter one type: self.get(reverse...)
            if self.filter_some_get_usage(node):
                return

            attri_list = []

            # Get the attribute list on the left.  `node.func` is the Attribute node.
            self.get_attr_list(node.func, attri_list)
            names = self.get_names(attri_list[::-1])

            # Get the list of column = val on the right hand side of the table.get(col1=, col2=)
            cols = helper.get_column_lists(node)
            if not cols:
                return

            self.cols += cols
            if get_type == "get":
                class_name, table_name = self.track_class_of_attr_list(attri_list[::-1])
            elif get_type == "get_obj":
                class_name, table_name = self.get_object_or_404_get_model(node)
            else:
                return

            if (not class_name) or (not table_name):
                self.attribute_list.append(".".join(names) + "." + ",".join(self.cols))
            else:
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(self.cols),
                        "usage": ".".join(names),
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": "get_type",
                        "file": self.filepath,
                        "extra": self.extra,
                    }
                )

        self.generic_visit(node)

    def filter_some_get_usage(self, node):
        # Cannot be url related.
        try:
            if node.args[0].value == "/":
                return 1
        except Exception as e:
            pass

    #
    # Test if the call attr is the keyword.
    #
    # @return, str, "get" or "get_obj". None if not match
    #
    def _is_get(self, node):

        # ['get', "get_or_create", 'update_or_create']
        try:
            if (node.func.attr in UniqueFinderGet.GET_KEYWORDS) and len(
                node.keywords
            ) > 0:
                return "get"

        except Exception as e:
            pass

        # ['get_object_or_404', "get_object_for_this_type"]
        try:
            if (
                hasattr(node.func, "id")
                and node.func.id in UniqueFinderGet.GET_KEYWORDS_ID
            ) or (
                hasattr(node.func, "attr")
                and node.func.attr in UniqueFinderGet.GET_KEYWORDS_ID
            ):
                return "get_obj"
        except Exception as e:
            pass

        return None


#######################
## M2M Keyword pattern for constraints.
#######################
class UniqueFinderM2M(basic_traverse.AttrAnalysis):

    M2M_KEYWORDS = ["add", "delete"]

    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(UniqueFinderM2M, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.all_attrlists = []
        self.task_type = "unique"

    #
    # Call nodes for pattern2: Table.M2M_field.add/remove
    #
    def visit_Call(self, node):
        # Clean it every time. Or the provious will polute it.
        self.cols = []
        self.extra = ""

        if node in self.visited_nodes:
            return

        self.visited_nodes.append(node)

        if self._is_M2M_keyword(node):
            attri_list = []

            # Get the attribute list on the left.  `node.func` is the Attribute node.
            self.get_attr_list(node.func, attri_list)
            names = self.get_names(attri_list[::-1])

            class_name, related_model_name, table_name = self.track_M2M_attr_list(
                attri_list[::-1]
            )

            self.cols.append(class_name.lower() + "_id")

            if (not class_name) or (not table_name):
                if class_name != None:
                    self.attribute_list.append(
                        ".".join(names) + "." + ",".join(self.cols)
                    )
            else:
                self.cols.append(related_model_name.lower() + "_id")
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(self.cols),
                        "usage": ".".join(names),
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": "M2M",
                        "file": self.filepath,
                        "extra": self.extra,
                    }
                )

        self.generic_visit(node)

    #
    # Test if is keyword.  (Query API)
    #
    def _is_M2M_keyword(self, node):
        try:
            if (
                (node.func.attr in UniqueFinderM2M.M2M_KEYWORDS)
                and isinstance(node.func.ctx, ast.Load)
                and isinstance(node.args[0].ctx, ast.Load)
                and isinstance(node.func.value, ast.Attribute)
            ):
                return True
        except Exception as e:
            return False


#######################
## Complex check-then-create-or-raise pattern for unique constraints.
## E.g.,
## wishlists = request.user.wishlists.all()[:1]
## if not wishlists:
##    return request.user.wishlists.create()
#######################
class UniqueFinderCheckThenAction(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(UniqueFinderCheckThenAction, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "unique_complex"

    #
    # Start from If node, go to three type of patterns.
    #
    #
    def visit_If(self, node):
        if node in self.visited_nodes:
            return

        self.visited_nodes.append(node)
        self.cols = []

        flag = ""
        if isinstance(node.test, ast.Compare):
            check_1, var_node = self._check_length_compare_with_0_1(node.test)
            if check_1:
                flag = "_check_length_compare_with_0_1"

        if isinstance(node.test, ast.Call):
            check_2, var_node = self._check_qs_exists(node.test)
            if check_2:
                flag = "_check_qs_exists"

        if isinstance(node.test, ast.UnaryOp):
            check_3, var_node = self._check_qs_UnaryOp(node.test)
            if check_3:
                flag = "_check_qs_UnaryOp"

        # Check another branch
        another_branch_check = False
        if flag:
            exc_in_branch, exc_lineno = self._check_exc_in_branches(node)
            is_exc_in_branch = True if exc_in_branch else False
            create_in_branch, create_lineno = self._check_create_in_branches(node)
            is_create_in_branch = True if create_in_branch else False

            another_branch_check = is_exc_in_branch | is_create_in_branch
            # Special case for not ..., need change this
            # 1) if not var : create
            # 2) if not objects.exist() : create
            if "check_3" in locals() and check_3 and exc_in_branch == "body":
                another_branch_check = False
            # if qs.exist(): xx.save()
            if "check_2" in locals() and check_2 and is_create_in_branch:
                another_branch_check = False

        if another_branch_check:
            # Detect the model and col.
            final_check = False
            attri_list = []
            self.get_attr_list(var_node, attri_list)
            names = self.get_names(attri_list[::-1])

            # When the node is like Model.objects.filter(column=col_id).exists():
            if isinstance(var_node, ast.Call):
                class_name, table_name = self.track_class_of_attr_list(attri_list[::-1])
                if class_name:
                    # Get the list of column = val on the right hand side of the table.get(col1=, col2=)
                    self.cols += helper.get_column_lists(var_node)
                    if self.cols:
                        final_check = True
                else:
                    pass

            # When the node is qs.exists():
            # Or the node is qs inside `if len(qs) > 0`:
            elif isinstance(var_node, ast.Name) or isinstance(var_node, ast.Attribute):
                # Note the columns are added inside the tracking to self.cols.
                class_name, table_name = self.track_class_of_attr_list(attri_list[::-1])
                if class_name:
                    final_check = True

            # Final persist results.
            if final_check:
                extra = "exc:" + str(exc_lineno) + " creat:" + str(create_lineno)
                result = {
                    "class": class_name,
                    "table": table_name,
                    "column": ",".join(self.cols),
                    "usage": ".".join(names),
                    "lineno": str(self.init_lineno + node.lineno),
                    "source": flag,
                    "file": self.filepath,
                    "extra": extra,
                }
                self.extract_constraints.append(result)
            else:
                self.attribute_list.append(
                    ".".join(names)
                    + "."
                    + ",".join(self.cols)
                    + self.filepath
                    + str(self.init_lineno + node.lineno)
                )

        # Need to keep visit the nodes under this IF subtree.
        self.generic_visit(node)

    #
    # Check if there is a compare node comare length with 0 or 1.
    # @node, Compare node.
    # E.g., if len(qs) > 0
    #
    def _check_length_compare_with_0_1(self, node):
        try:
            if isinstance(node, ast.Compare):
                # in left and comparators[0], one should be variable for len(qs), another should be num = 1 / 0.
                # GET two things: var_node and num_value
                var_node = node.left
                num_value = self._get_constant_value(node.comparators[0])
                if num_value == -1:
                    num_value = self._get_constant_value(node.left)
                    var_node = node.comparators[0]

                # If none of them is a constant value 0 ,1 (or not value so becomes -1), directly return False.
                if num_value not in [0, 1]:
                    return False, None
                # Can further consider if > 0 or > 1 or == 0 , etc.
                # Now just know it compares with 0 OR 1.

                # WHen the node is like len(qs).  (Full pattern is: if len(qs) > 0:)
                if (
                    hasattr(var_node, "func")
                    and self.get_attr_name_name(var_node.func) == "len"
                ):
                    return True, var_node.args[0]
                else:
                    return True, var_node

        except Exception as e:
            print(
                "Exception in _check_length_compare_with_0_1",
                e,
                ast.dump(node),
                str(self.init_lineno + node.lineno),
            )
            return False, None

    def _get_constant_value(self, node):
        if isinstance(node, ast.Constant):
            return node.value
        return -1

    #
    # E.g., if qs.exists():
    #
    def _check_qs_exists(self, node):
        try:
            if isinstance(node, ast.Call):
                if hasattr(node.func, "attr") and node.func.attr == "exists":
                    var_node = node.func.value
                    return True, var_node

            return False, None

        except Exception as e:
            print(
                "Exception in _check_qs_exists",
                e,
                ast.dump(node),
                str(self.init_lineno + node.lineno),
            )
            return False, None

    #
    # E.g., if not wishlists:
    #
    def _check_qs_UnaryOp(self, node):
        try:
            if isinstance(node, ast.UnaryOp):
                # print(ast.dump(node))
                if isinstance(node.op, ast.Not):
                    var_node = node.operand
                    # if not qs.exist():
                    exist, tmpnode = self._check_qs_exists(var_node)
                    if exist:
                        return True, tmpnode
                    if not hasattr(var_node, "value"):
                        return True, var_node

            return False, None

        except Exception as e:
            print(
                "Exception in _check_qs_exists",
                e,
                ast.dump(node),
                str(self.init_lineno + node.lineno),
            )
            return False, None

    #
    # Check if one branch of the `IF` node is exception . The branch can be current bramch, else branch, or after the IF node path.
    # @node, the IF node.
    # return, where is the exception, "body", "else", "sibling". Or False
    # E.g., raise exception
    #
    def _check_exc_in_branches(self, node):
        try:
            for body in node.body:
                if isinstance(body, ast.Raise):  # Or logger.warning / error
                    return "body", self.init_lineno + body.lineno
        except Exception as e:
            pass

        try:
            for elsenode in node.orelse:
                if isinstance(elsenode, ast.Raise):
                    return "else", self.init_lineno + elsenode.lineno
        except Exception as e:
            pass

        try:
            for child in ast.iter_child_nodes(node.parent):
                if isinstance(child, ast.Raise):
                    return "sibling", self.init_lineno + child.lineno
        except Exception as e:
            pass

        errornode = basic_traverse.CallErrorNodeFromSubTree()
        errornode.visit(node)

        if errornode.errornodes:
            return "call_error", self.init_lineno + errornode.errornodes[0].lineno

        return False, -1

    #
    # Check if current node is a create()
    # @node, the Call node. Or with some of its parents.
    # E.g., return request.user.wishlists.create()
    #
    def _check_create_in_branches(self, node):
        try:
            # print("ast.dump node: %s" % ast.dump(node))
            for subnode in ast.walk(node):
                if isinstance(subnode, ast.Call):
                    if hasattr(subnode.func, "attr") and subnode.func.attr in [
                        "create",
                        "save",
                    ]:
                        return True, self.init_lineno + subnode.lineno

            return False, -1

        except Exception as e:
            return False
