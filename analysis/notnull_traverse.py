#!/usr/bin/env python
# coding: utf-8
import gast as ast, ast as old_ast
import sys
from . import helper
from . import basic_traverse

if sys.version_info.major >= 3:
    from importlib import reload
import builtins

reload(helper)
reload(basic_traverse)

#######################
## constraints for not null, filed/method invocation
#######################
class NotNullFinder(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(NotNullFinder, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "null"

    def visit_Attribute(self, node):
        self.cols = []
        self.extra = ""
        self.extra_note = ""
        self.has_check = ""
        self.source = ""

        if node in self.visited_nodes:
            return
        self.visited_nodes.append(node)

        parent = node.parent

        attri_list = []
        self.get_attr_list(node, attri_list)
        names = self.get_names(attri_list[::-1])

        track_attr_list, source = self.determin_type_of_pattern(node.parent, attri_list)

        if len(track_attr_list) < 2:
            return

        self.get_results_from_attr_list(track_attr_list, node, names)
        self.generic_visit(node)

    def visit_Call(self, node):
        if node in self.visited_nodes:
            return
        self.visited_nodes.append(node)

        if self.get_attr_name_name(node.func) != "F":
            self.generic_visit(node)
            return

        self.cols = []
        self.extra = ""
        self.extra_note = ""
        self.has_check = ""
        self.source = ""

        parent = node.parent
        if isinstance(parent, ast.BinOp) and not isinstance(parent.op, ast.Mod):  #
            self.source = "operator"
        else:
            self.generic_visit(node)
            return

        col_name = node.args[0]
        class_name, table_name = None, None
        if isinstance(col_name, ast.Constant):
            try:
                attri_list = []
                self.get_attr_list(node.parent.parent.parent.func.value, attri_list)
                attri_list = [col_name] + attri_list
                names = self.get_names(attri_list[::-1])
                class_name, table_name = self.track_class_of_attr_list_for_null(
                    attri_list[::-1]
                )
            except:
                pass

            if not class_name:
                class_name, table_name = self.try_get_table_from_field(
                    col_name.value, None
                )
            if class_name and table_name:
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": col_name.value,
                        "usage": "F(" + col_name.value + ")",
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": self.source,
                        "file": self.filepath,
                        "extra": self.extra,
                        "has_check": "0",
                    }
                )
        self.generic_visit(node)

    #
    # From the parent node of attr list, determine if it belongs to a type of interest.
    #
    def determin_type_of_pattern(self, parent, attri_list):
        names = self.get_names(attri_list[::-1])
        # If the parent is operator / compare, the whole attr list be not null
        if (
            (isinstance(parent, ast.BinOp) and not isinstance(parent.op, ast.Mod))
            or (
                isinstance(parent, ast.Compare)
                and isinstance(
                    parent.ops[0], (ast.Gt, ast.Lt, ast.GtE, ast.LtE, ast.Eq)
                )
            )
            or (isinstance(parent, ast.AugAssign))
        ):  #
            track_attr_list = attri_list
            if isinstance(parent, ast.Compare) and isinstance(parent.ops[0], ast.Eq):
                self.source = "eq"
            else:
                self.source = "operator"
        #
        # Function call parameters. E.g., int(a.b)
        # Seems can only focus on built-in functions.
        #
        elif (
            isinstance(parent, ast.Call)
            and hasattr(parent, "func")
            and isinstance(parent.func, ast.Name)
            and (parent.func.id in dir(builtins))
            and (parent.func.id not in ["str", "bool"])
        ):
            track_attr_list = attri_list
            self.source = "funcCall"
        else:
            # E.g., self.product.get_title()  -> product in self model is not null
            # E.g., self.wishlist.name    -> wishlist in self model is not null
            track_attr_list = attri_list[1:]

            if (
                attri_list
                and hasattr(attri_list[0], "parent")
                and hasattr(attri_list[0].parent, "func")
            ):
                self.source = "method"
                if self.get_attr_name_name(attri_list[0].parent.func) in [
                    "exist",
                    "add",
                    "all",
                    "using",
                    "exclude",
                ]:
                    self.extra_note = "filter"
            else:
                self.source = "field"

        return track_attr_list, self.source

    #
    # Store the results.
    #
    def get_results_from_attr_list(self, track_attr_list, node, names):
        # Input  here: self.wishlist
        class_name, table_name = self.track_class_of_attr_list(track_attr_list[::-1])

        if (not class_name) or (not table_name):
            self.attribute_list.append(".".join(names))
        else:
            last_attr_name = self.get_attr_name_name(track_attr_list[0])
            cols = self.get_col_from_model(last_attr_name, class_name, table_name)
            cols_id = self.get_col_from_model(
                last_attr_name.replace("_id", ""), class_name, table_name
            )
            try:
                if self.extra_note == "filter":
                    self.extra = "filter"
            except:
                pass

            if not cols:
                cols = cols_id
            if (not cols) and (not cols_id):
                fk_exist, class_name, table_name, cols = self.try_m2m_related_field(
                    last_attr_name, class_name, table_name
                )
                if fk_exist:
                    self.extract_constraints.append(
                        {
                            "class": class_name,
                            "table": table_name,
                            "column": cols,
                            "usage": ".".join(names),
                            "lineno": str(self.init_lineno + node.lineno),
                            "source": "fk",
                            "file": self.filepath,
                            "extra": self.extra,
                            "has_check": "0",
                        }
                    )
                else:
                    self.attribute_list.append(".".join(names))
            else:
                # Consider the usage of field with checks before.
                # Label such cases in the self.has_check = "has_check"
                self.has_check = self.filter_check_exists(
                    ".".join(self.get_names(track_attr_list[::-1])), node
                )
                # Store the record
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(cols),
                        "usage": ".".join(names),
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": self.source,
                        "file": self.filepath,
                        "extra": self.extra,
                        "has_check": self.has_check,
                    }
                )

    # Some fields are `related_names` or `many_to_many_fields`.
    # Once detected, we need to further generate the correct constraints.
    def try_m2m_related_field(self, last_attr_name, class_name, table_name):
        model_class = self.model_class_info
        sub_model_class = model_class[
            (model_class["field"] == last_attr_name)
            & (model_class["model"] == class_name)
            & (model_class["table"] == table_name)
        ]
        #
        # For the case of foreign key
        #
        model_with_field = sub_model_class[
            sub_model_class["field_type"] == "ManyToOneRel"
        ]
        if model_with_field.shape[0] >= 1:
            remote_model = list(model_with_field.related_model)[0]
            cur_model = list(model_with_field.table)[0]
            subdf = model_class[
                (model_class["related_names"] == last_attr_name)
                & (model_class["table"] == remote_model)
                & (model_class["related_model"] == cur_model)
            ]

            if subdf.empty:
                return False, None, None, None
            else:
                return (
                    1,
                    list(subdf.model)[0],
                    list(subdf.table)[0],
                    list(subdf.field)[0],
                )

        #
        # For the case of ManyToManyRel
        #
        model_with_field = sub_model_class[
            sub_model_class["field_type"] == "ManyToManyRel"
        ]
        if model_with_field.shape[0] >= 1:
            remote_model = list(model_with_field.related_model)[0]
            cur_model = list(model_with_field.model)[0]

            subdf = model_class[
                (model_class["related_names"] == last_attr_name)
                & (model_class["model"] == remote_model)
            ]

            if not subdf.empty:
                through_model = list(subdf.through_model)[0]
                m2mfields = model_class[
                    (model_class["is_m2m_field"] == "yes")
                    & (model_class["model"] == through_model)
                ]

                for index, row in m2mfields.iterrows():
                    print("200: ", row["model"], row["field"], through_model)

                    return 1, row["model"], row["table"], row["field"]

        #
        # For the case of ManyToManyField
        #
        model_with_field = sub_model_class[
            sub_model_class["field_type"] == "ManyToManyField"
        ]
        if model_with_field.shape[0] >= 1:
            through_model = list(model_with_field.through_model)[0]
            m2mfields = model_class[
                (model_class["is_m2m_field"] == "yes")
                & (model_class["model"] == through_model)
            ]

            for index, row in m2mfields.iterrows():
                # print("214: ", row['model'], row['field'], through_model)
                return 1, row["model"], row["table"], row["field"]

        return False, None, None, None

    # Find the functiondef node starting from the current attr node
    # @return, the FunctionDefNode.
    def get_FuncDef_from_node(self, node):
        cur = node
        while 1:
            if isinstance(cur, ast.FunctionDef):
                return cur
            if hasattr(cur, "parent"):
                cur = cur.parent
            else:
                return None

    # Check if the not-null-check exists.
    # Start from the functiondef.
    # @return str, the label.
    def filter_check_exists(self, attr, node):
        funcdef = self.get_FuncDef_from_node(node)
        if not funcdef:
            return "0"

        lineno = self.init_lineno + funcdef.lineno
        if lineno == 0:
            raise Exception("filter_check_exists: lineno = 0")

        # Do the checkfinder in this function.
        check_attrs = CheckFinderForFilterNull(
            self.class_name,
            self.udchains,
            self.ancestors,
            self.init_lineno,
            self.model_class_info,
            self.filepath,
        )
        check_attrs.visit(funcdef)

        self.checked_attributes = check_attrs.checked_attributes

        for item in self.checked_attributes:
            if item["name"] == attr and item["node"] == node:
                return "-1"

        # Exact at difference place -> check!
        # If not exist, sub match, xxxx
        for item in self.checked_attributes:
            if item["name"] == attr and item["node"] != node:
                return str(lineno)

        subatt = attr.split(".")[-1].replace("_id", "")
        for item in self.checked_attributes:
            if item["name"] == subatt and item["node"] != node:
                return str(lineno)

        return "0"


# If check exists before the action, then need filter them.
# E.g., if self.num_in_stock is None: return 0
#          return self.num_in_stock - self.num_allocated
#
# Steps:
# 0. Start from the attr list, find the currrent function.
# 1. We find the If pattern directlyin the same func body with the pattern.
# 2. Check the attr list is the same.
#
class CheckFinderForFilterNull(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        self.checked_attributes = []
        super(CheckFinderForFilterNull, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )

    #
    # self.num_in_stock is None
    # As long as compare with None,
    # Do not differentiate several cases:
    # 1) comes form the if node.  2) In assert, assert self.num_in_stock is None
    #
    def visit_Compare(self, node):

        checknode = basic_traverse.CompareNoneNodeFromSubTree(usedplace="if_check")
        checknode.visit(node)

        var_nodes = checknode.checknodes
        # Has the check pattern, then get the attris from the var_node
        if var_nodes:
            for varnode in var_nodes:
                # print("visit_Compare: ", self.init_lineno + node.lineno)
                self.add_to_attrlist(varnode)

        self.generic_visit(node)

    #
    # E.g., if self.num_in_stock: return 0
    #
    # E.g., if self.num_in_stock and self.b:
    #
    def visit_If(self, node):
        if not isinstance(node.test, ast.Compare):
            attr = basic_traverse.AllAttrNodesFromSubTree()
            attr.visit(node.test)

            for attrnode in attr.subattributes:
                if not isinstance(attrnode.parent, ast.Compare):
                    self.add_to_attrlist(attrnode)

        self.generic_visit(node)

    #
    # E.g., app_id = export_file.app.id if export_file.app else None
    #
    def visit_IfExp(self, node):
        if not isinstance(node.test, ast.Compare):
            attr = basic_traverse.AllAttrNodesFromSubTree()
            attr.visit(node.test)

            for attrnode in attr.subattributes:
                self.add_to_attrlist(attrnode)

        self.generic_visit(node)


#
# If check exists before the action, then need filter them.
# E.g., if self.num_in_stock is None: raise Exception
#        assert prereg_user.referred_by is not None
#
# Rules -  Find the following patterns:
# 1) For if conditions
# First, start from a IF node, find the following patterns:
#    (self.a is None) or (self.b == None) or (self.b without any comparison)
#  AND     1.1 raise Exception (raise Exception()) or (logger.info()) or (message.error())
#      OR  1.2 self.a = xxx,  assign a value to the variable.
#
# 2) For ASSERTs
#    Assert self.a is None   Assert self.b == None
#
class NotNullCheckExecptionPattern(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(NotNullCheckExecptionPattern, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "null"
        self.checked_attributes = []

    # Part 1.1: have exception or error in one path of IF nodes.
    def error_in_subtree(self, node):
        result = []
        error_nodes = []
        # Rasise exception
        raisenode = basic_traverse.AllRaiseNodesFromSubTree()
        raisenode.visit(node)

        if raisenode.raisenodes:
            result.append("raise")
            error_nodes += raisenode.raisenodes
        # Call Error node, E.g., logger.error()
        errornode = basic_traverse.CallErrorNodeFromSubTree()
        errornode.visit(node)

        if errornode.errornodes:
            result.append("error")
            error_nodes += errornode.errornodes

        return result, error_nodes

    # Part 1.2: have value assignment in if condition
    def assign_in_subtree(self, node):
        assign_attr = basic_traverse.AllAssignNodesFromSubTree()
        assign_attr.visit(node)

        if assign_attr.assign_attr_nodes:
            return assign_attr.assign_attr_nodes
        return None

    # Part 2: have attr with None comparison.
    def attr_in_subtree(self, node):
        checknode = basic_traverse.CompareNoneNodeFromSubTree(usedplace="if_raise")
        checknode.visit(node)

        var_nodes = checknode.checknodes
        # Has the check pattern, then get the attris from the var_node
        return var_nodes

    # Find the functiondef node starting from the current attr node
    # Node -> start find the IF node from the current input node.
    # @return, the FunctionDefNode.
    def get_If_from_node(self, node):
        cur = node
        while 1:
            if isinstance(cur, ast.If):
                return cur
            if hasattr(cur, "parent"):
                cur = cur.parent
            else:
                return None

    # Find the functiondef node starting from the current attr node
    # @return, the FunctionDefNode.
    def get_FuncDef_from_node(self, node):
        cur = node
        while 1:
            if isinstance(cur, ast.FunctionDef):
                return cur
            if hasattr(cur, "parent"):
                cur = cur.parent
            else:
                return None

    #
    # E.g., if self.num_in_stock is None: raise Exception
    #
    def main_if_logic(self, node):
        # Many cases start from IF node.
        # We should ignore these.
        # Start from node.parent
        if self.get_If_from_node(node.parent):
            return
        func_node = self.get_FuncDef_from_node(node)
        if func_node:
            if "clean_" in func_node.name:
                return

        error_rst, error_nodes = self.error_in_subtree(node)
        assign_attr_nodes = self.assign_in_subtree(node)

        if error_rst:
            attr_nodes = self.attr_in_subtree(node)

            for attr_node in attr_nodes:
                # Match the pattern, record it.
                self.get_results(attr_node, node, "if_raise")
            return

        if assign_attr_nodes:
            attr_nodes = self.attr_in_subtree(node)
            for attr_node in attr_nodes:
                # Get the name of the assign node. self.a = ...
                assign_node_list = []
                self.get_attr_list(assign_attr_nodes[0], assign_node_list)
                assign_node_name = self.get_names(assign_node_list[::-1])

                # Get the name of the attr node in comparison.  if self.a is none ...
                attr_node_list = []
                self.get_attr_list(attr_node, attr_node_list)
                attr_node_name = self.get_names(attr_node_list[::-1])

                # If the two names are the same, record it.
                if assign_node_name == attr_node_name:
                    self.get_results(attr_node, node, "if_assign")

    def visit_If(self, node):
        self.main_if_logic(node)
        self.generic_visit(node)

    def visit_IfExp(self, node):
        self.main_if_logic(node)
        self.generic_visit(node)

    #
    # E.g., assert prereg_user.referred_by is not None
    #
    def visit_Assert(self, node):
        # Many cases start from IF node.
        # We should ignore these.
        if self.get_If_from_node(node):
            return
        func_node = self.get_FuncDef_from_node(node)
        if func_node:
            if "clean_" in func_node.name:
                return

        checknode = basic_traverse.CompareNoneNodeFromSubTree(usedplace="if_raise")
        checknode.visit(node)

        var_nodes = checknode.checknodes

        if checknode.optype in ["isnot", "noteq"]:
            for attr_node in var_nodes:
                # Match the pattern, record it.
                self.get_results(attr_node, node, "assert")

        self.generic_visit(node)

    #
    # Record the result from attr_list.
    #
    def get_results(self, attr_node, node, error_type):
        self.extra = error_type

        track_attr_list = []
        self.get_attr_list(attr_node, track_attr_list)
        names = self.get_names(track_attr_list[::-1])

        if not track_attr_list:
            return

        source = error_type

        class_name, table_name = self.track_class_of_attr_list_for_null(
            track_attr_list[::-1]
        )

        if (not class_name) or (not table_name):
            self.attribute_list.append(".".join(names))
        else:
            last_attr_name = self.get_attr_name_name(track_attr_list[0])
            cols = self.get_col_from_model(last_attr_name, class_name, table_name)
            cols_id = self.get_col_from_model(
                last_attr_name.replace("_id", ""), class_name, table_name
            )
            if not cols:
                cols = cols_id
            if (not cols) and (not cols_id):
                self.attribute_list.append(".".join(names))
            else:
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(cols),
                        "usage": ".".join(names),
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": source,
                        "file": self.filepath,
                        "extra": self.extra,
                        "has_check": "0",
                    }
                )


############################
##  Some pattern clearly shows the field can be NULL.
##  We can filter them.
#  E.g., user_profile.last_reminder = None
#  E.g., kw value is none
#  E.g., Q(max_value__isnull=False)
############################
class NullablePattern(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(NullablePattern, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "null"

    def visit_Assign(self, node):
        self.cols = []
        self.extra = ""
        self.has_check = ""
        source = "notnull"

        if node in self.visited_nodes:
            return
        self.visited_nodes.append(node)

        track_attr_list = None
        if hasattr(node, "targets") and isinstance(node.targets[0], ast.Attribute):
            # rhs is None
            num_value_node = node.value
            if isinstance(num_value_node, ast.Constant):
                num_value = num_value_node.value
                if num_value == None:
                    attri_list = []
                    self.get_attr_list(node.targets[0], attri_list)
                    names = self.get_names(attri_list[::-1])

                    source = "filter"
                    track_attr_list = attri_list

        if not track_attr_list or len(track_attr_list) < 2:
            return

        # Input  here: self.wishlist
        # Output here:
        class_name, table_name = self.track_class_of_attr_list_for_null(
            track_attr_list[::-1]
        )

        if (not class_name) or (not table_name):
            self.attribute_list.append(".".join(names))
        else:
            last_attr_name = self.get_attr_name_name(track_attr_list[0])
            cols = self.get_col_from_model(last_attr_name, class_name, table_name)
            cols_id = self.get_col_from_model(
                last_attr_name.replace("_id", ""), class_name, table_name
            )
            if not cols:
                cols = cols_id
            if (not cols) and (not cols_id):
                self.attribute_list.append(".".join(names))
            else:
                self.has_check = "0"
                # Store the record
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(cols),
                        "usage": ".".join(names),
                        "lineno": str(self.init_lineno + node.lineno),
                        "source": source,
                        "file": self.filepath,
                        "extra": self.extra,
                        "has_check": self.has_check,
                    }
                )

        self.generic_visit(node)

    def visit_keyword(self, node):
        self.cols = []
        self.extra = ""
        self.has_check = ""

        if node in self.visited_nodes:
            return
        self.visited_nodes.append(node)

        attri_list = []
        if hasattr(node, "value"):
            # rhs is None
            if self.get_attr_name_name(node.value) == None:
                attri_list.append(node.arg)

        if attri_list == []:
            return

        # Guess based on the field name.
        try:
            class_name, table_name = self.try_get_table_from_field(attri_list[0], None)
            if class_name and table_name:
                self.extra = "4-guess:" + str(self.extra)
        except Exception as e:
            class_name, table_name = None, None

        if class_name and table_name:
            last_attr_name = attri_list[0]
            cols = self.get_col_from_model(last_attr_name, class_name, table_name)
            cols_id = self.get_col_from_model(
                last_attr_name.replace("_id", ""), class_name, table_name
            )
            if not cols:
                cols = cols_id
            try:
                nodelineno = node.lineno
            except:
                nodelineno = 0
            if cols or cols_id:
                self.extract_constraints.append(
                    {
                        "class": class_name,
                        "table": table_name,
                        "column": ",".join(cols),
                        "usage": last_attr_name,
                        "lineno": str(self.init_lineno + nodelineno),
                        "source": "filter_default",
                        "file": self.filepath,
                        "extra": self.extra,
                        "has_check": "0",
                    }
                )

        self.generic_visit(node)
