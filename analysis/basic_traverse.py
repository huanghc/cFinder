#!/usr/bin/env python
# coding: utf-8
import gast as ast, ast as old_ast
import sys
import traceback
from . import helper

if sys.version_info.major >= 3:
    from importlib import reload
reload(helper)

#######################
## Get Class trees
#######################
class Class_tree(ast.NodeVisitor):
    def __init__(self):
        self.subtrees = []
        self.lineno = []

    def visit_ClassDef(self, node):
        subtree = {}
        subtree["lineno"] = node.lineno
        subtree["node"] = node
        subtree["name"] = node.name
        subtree["type"] = "class"
        self.subtrees.append(subtree)

    def visit_FunctionDef(self, node):
        subtree = {}
        subtree["lineno"] = node.lineno
        subtree["node"] = node
        subtree["name"] = node.name
        subtree["type"] = "func"
        self.subtrees.append(subtree)


def get_class_tree(module):
    class_tree = Class_tree()
    class_tree.visit(module)
    return class_tree.subtrees


#######################
## Get Django class_info
#######################
#
# Helper func
#
def get_related_names(keywords):
    for keyword in keywords:
        if keyword.arg == "related_name":
            return keyword.value.value
    return ""


#
# Helper func
#
def get_default(keywords):
    for keyword in keywords:
        if keyword.arg == "default":
            return keyword.value.value
    return ""


#
# Helper func
#
def gen_class_name(name, directory):
    # Special case - For Oscar, the class name may include the Abstract.
    return directory + "." + name.replace("Abstract", "")


#
# Helper func
#
def assign_parents_to_ast(root):
    for node in ast.walk(root):
        for child in ast.iter_child_nodes(node):
            child.parent = node


def get_attr_name_name(item):
    if isinstance(item, ast.Attribute):
        return item.attr
    elif isinstance(item, ast.Name):
        return item.id
    elif isinstance(item, str):
        return item
    else:
        return type(item)


#
# Helper func
# Basic traverse and get some nodes from sub tree.
#
# Get all attr nodes within a subtree, e.g. from a IF node.
# Used for NULL, input node is node.test (the if condition node)
#
class AllAttrNodesFromSubTree(ast.NodeVisitor):
    def __init__(self):
        self.visited_notes = []
        self.subattributes = []

    def visit_Attribute(self, node):
        # print('108', get_attr_name_name(node))
        if node.parent in self.visited_notes:
            self.visited_notes.append(node)
            return
        if node in self.visited_notes:
            return
        self.visited_notes.append(node)

        self.subattributes.append(node)
        self.generic_visit(node)


#
# Get all Raise  (Exception) nodes within a subtree, e.g. from a IF node.
# Used for NULL
#
class AllRaiseNodesFromSubTree(ast.NodeVisitor):
    def __init__(self):
        self.raisenodes = []

    def visit_Raise(self, node):
        self.raisenodes.append(node)
        self.generic_visit(node)


#
# Get all Raise  (Exception) nodes within a subtree, e.g. from a IF node.
# Used for NULL
#
class CallErrorNodeFromSubTree(ast.NodeVisitor):
    def __init__(self):
        self.errornodes = []

    def visit_Call(self, node):
        if hasattr(node, "func") and get_attr_name_name(node.func) in [
            "warn",
            "warning",
            "error",
            "exception",
            "critical",
        ]:
            self.errornodes.append(node)

        self.generic_visit(node)


#
# Get all Assign  (self.a = xxx) nodes within a subtree, e.g. from a IF node.
# Used for NULL
#
class AllAssignNodesFromSubTree(ast.NodeVisitor):
    def __init__(self):
        self.assign_attr_nodes = []

    def visit_Assign(self, node):
        if isinstance(node.targets[0], ast.Attribute):
            self.assign_attr_nodes.append(node.targets[0])
        self.generic_visit(node)


#
# Get all Raise  (Exception) nodes within a subtree, e.g. from a IF node.
# Used for NULL
#
class CompareNoneNodeFromSubTree(ast.NodeVisitor):
    def __init__(self, usedplace):
        self.checknodes = []
        self.optype = ""
        self.usedplace = usedplace  # if_check  # if_raise

    #
    # self.num_in_stock is None
    # As long as compare with None,
    # Do not differentiate several cases:
    # 1) comes form the if node.  2) In assert, assert self.num_in_stock is None
    #
    def visit_Compare(self, node):
        check, optype, var_node = self._check_compare_with_None(node)
        # Has the check pattern, then get the attris from the var_node
        if check:
            if (self.usedplace == "if_check") or (
                not (hasattr(node.parent, "op") and isinstance(node.parent.op, ast.And))
            ):
                self.optype = optype
                self.checknodes.append(var_node)

        self.generic_visit(node)

    #
    # Check if there is a check with None.
    # @node, Compare node.
    # E.g., if self.num_in_stock is None:
    #
    def _check_compare_with_None(self, node):
        try:
            var_node = node.left
            num_value_node = node.comparators[0]

            if isinstance(num_value_node, ast.Constant):
                num_value = num_value_node.value
                if num_value == None:
                    # 1) is none or 2) is not none:
                    return True, self._Compare_ops_type(node.ops[0]), var_node

            return False, None, None
        except Exception as e:
            print(
                "Exception in _check_compare_with_None",
                e,
                ast.dump(node),
                str(node.lineno),
            )
            return False, None, None

    # Input node is directly the node.ops[0] node, not the coompare node.
    # Return str, type.
    def _Compare_ops_type(self, node):
        if isinstance(node, ast.IsNot):
            return "isnot"
        elif isinstance(node, ast.Eq):
            return "eq"
        elif isinstance(node, ast.NotEq):
            return "noteq"
        else:
            return "Wrong!"

    #
    # Let's also include another kind that not involve the compare node
    #
    def visit_Attribute(self, node):
        # If not self.a: xxx
        # The last and means should not be inside another -  (not xxx) and (xxx).
        if hasattr(node.parent, "op") and isinstance(node.parent.op, ast.Not):
            if (
                hasattr(node.parent, "parent")
                and hasattr(node.parent.parent, "op")
                and isinstance(node.parent.parent.op, ast.And)
            ):
                return
            try:
                if isinstance(node.parent.parent.parent.op, ast.And):
                    return
            except:
                pass
            self.checknodes.append(node)
        # If self.a or xxx:
        elif isinstance(node.parent, ast.If) or (
            isinstance(node.parent, ast.BoolOp) and isinstance(node.parent.op, ast.Or)
        ):
            # Filter some cases that - if xxx: raise xxxx
            if not (
                isinstance(node.parent, ast.If)
                and isinstance(node.parent.body[0], ast.Raise)
            ):
                self.checknodes.append(node)
        else:
            pass

        self.generic_visit(node)


#
# For a list of attributes, get the init table and column
#
class AttrAnalysis(ast.NodeVisitor):

    skip_attr_list = ["select_related", "prefetch_related", "active", "__class__"]

    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        self.class_name = class_name
        self.visited_nodes = []
        self.udchains = udchains
        self.ancestors = ancestors
        self.model_class_info = model_class_info
        self.extract_constraints = []
        self.attribute_list = []
        self.init_lineno = init_lineno
        self.filepath = filepath
        self.extra = ""
        self.cols = []
        self.clear_def = ""
        self.task_type = ""
        self.def_use_nodes = []
        self.defuse_num = 0

    def get_attr_list(self, node, attri_list):
        if isinstance(node, ast.Attribute) and (
            isinstance(node.ctx, ast.Load) or isinstance(node.ctx, ast.Store)
        ):
            if not (hasattr(node, "attr") and node.attr in AttrAnalysis.skip_attr_list):
                attri_list.append(node)
            # iterate
            self.get_attr_list(node.value, attri_list)
        # Name usually is the last one
        elif isinstance(node, ast.Name):
            if node.id == "models" or node.id == "os":
                return
            else:
                attri_list.append(node)
        # sometimes is a function call
        elif isinstance(node, ast.Call):
            self.get_attr_list(node.func, attri_list)
        # Sometimes Subscript
        elif isinstance(node, ast.Subscript):
            self.get_attr_list(node.value, attri_list)

    def get_names(self, attri_list):
        names = []
        for item in attri_list:
            att_name = self.get_attr_name_name(item)
            names.append(att_name)
        return names

    def get_names_without_last(self, attri_list):
        names = []
        for item in attri_list[:-1]:
            names.append(self.get_attr_name_name(item))
        return names

    def get_attr_name_name(self, item):
        if isinstance(item, ast.Attribute):
            return item.attr
        elif isinstance(item, ast.Name):
            return item.id
        elif isinstance(item, ast.Constant):
            return item.value
        elif isinstance(item, str):
            return item
        else:
            return type(item)

    #
    # Get the class object from the app and class name.
    #
    # @class_name str,  the class_name get from the AST.
    # E.g., 'basket.LineAttribute'
    #
    # @return, cannot return class obj. Let's return that row in model_class_info instead.
    #
    def get_modelobj_from_app_and_model(self, app, model, table_name):

        # Try with model itself. In case the app not in the installed_apps.
        tmp = self.model_class_info[
            (self.model_class_info["model"] == model)
            & (self.model_class_info["app"] == app)
        ]
        if not tmp.empty:
            return tmp

        # Failover, in case the app is none. Then get the app by model name.
        tmp = self.model_class_info[
            (self.model_class_info["model"] == model)
            & (self.model_class_info["table"] == table_name)
        ]
        if tmp.empty:
            tmp2 = self.model_class_info[(self.model_class_info["model"] == model)]
            if not tmp2.empty:
                return self.get_modelobj_from_app_and_model(
                    tmp2.iloc[0]["app"], model, tmp2.iloc[0]["table"]
                )
        else:
            return self.get_modelobj_from_app_and_model(tmp.iloc[0]["app"], model, "")

        return None

    #
    # Check below
    #
    def track_class_of_attr_list(self, attri_list):
        self.defuse_num = 0
        return self.track_class_of_attr_list_for_null(attri_list)

    #
    # Track the last class of the attr_list to get the table。
    #
    # @return (str, str),  the class name, the table name
    #
    # #attri_list E.g., self.attribute.option_group.options.get
    #
    def track_class_of_attr_list_for_null(self, attri_list):
        if not attri_list:
            return None, None
        # Case 1： Model.objects.get
        try:
            if self.get_attr_name_name(attri_list[0]) != "self":
                model_name, table_name = self.get_model_from_list(attri_list)
                if model_name and table_name:
                    self.extra = "1-Model"
                    return model_name, table_name
        except Exception as e:
            print(traceback.format_exc())

        # Case 2: Start with self. E.g., self.xxx.wishlist
        try:
            if self.get_attr_name_name(attri_list[0]) == "self":
                if self.class_name:
                    app, model = self.class_name.split(".")
                    model_obj = self.get_modelobj_from_app_and_model(app, model, "")
                    # The returned value can be either DF or None
                    if type(model_obj).__name__ == "DataFrame":
                        class_name, table_name = self.get_table_from_list_with_starter(
                            attri_list[1:-1], model_obj
                        )
                        if class_name and table_name:
                            self.extra = "2-self"
                            return class_name, table_name
        except Exception as e:
            print(traceback.format_exc())

        # Special stop at def-use case.
        try:
            if self.defuse_num == 1:
                class_name, table_name = self.try_get_table_from_attrs(
                    attri_list, final=True
                )
                if class_name and table_name:
                    self.extra = "4-guess:" + self.extra
                    return class_name, table_name
                else:
                    return None, None
        except Exception as e:
            print(traceback.format_exc())

        # Case 3: try to get the model from def use chain.
        try:
            model_node = attri_list[0]
            model_name, db_table_name = self.find_obj_model_from_def_use(model_node)
            if model_name:
                # Once get the model of the first obj, keep the same operation as Case 2.
                model_obj = self.get_modelobj_from_app_and_model(
                    "", model_name, db_table_name
                )
                class_name, table_name = self.get_table_from_list_with_starter(
                    attri_list[1:-1], model_obj
                )
                if class_name and table_name:
                    self.extra = "3-defuse"
                    return class_name, table_name
        except Exception as e:
            print(traceback.format_exc())

        # Case 4: Can only do the guess based on the field name.
        # For view functions start with self. then the self. need to be ignored as the self is not the expected class.
        # SUch self.xxx always come from def dispatch(): self.xxx = get_object_or_404(Model, xxx)
        try:
            if (
                self.get_attr_name_name(attri_list[0]) == "self"
                and self.task_type == "null"
            ):
                attri_list = attri_list[1:]

            if self.get_attr_name_name(attri_list[-1]) == "get":
                attri_list = attri_list[:-1]

            class_name, table_name = self.try_get_table_from_attrs(attri_list)
            if class_name and table_name:
                self.extra = "4-guess:" + str(self.extra)
                return class_name, table_name
        except Exception as e:
            print(traceback.format_exc())

        return None, None

    #
    # With starter obj, and list of attris, get the final Model/Table.
    #
    # @attri_list,
    # @model_obj, the starter model, in a dataframe form. This is required and need to make sure it's valid.
    #
    # @return (str, str),  the class name, the table name
    #
    def get_table_from_list_with_starter(self, attri_list, model_obj):
        if type(model_obj).__name__ != "DataFrame":
            return "", ""

        rst_model = list(model_obj["model"])[0]
        rst_table = list(model_obj["table"])[0]
        mc = self.model_class_info

        for idx, node in enumerate(attri_list):
            # Get the model from the name
            try:
                name = self.get_attr_name_name(node)
                tmp = mc[(mc["table"] == rst_table) & (mc["field"] == name)]

                if not tmp.empty:
                    tmp_table = list(tmp["related_model"])[0]
                    if tmp_table:
                        rst_table = tmp_table
                        rst_model = list(mc[mc["table"] == rst_table].model)[0]

                        # e.g., self.review.votes -> rst_table = vote  name=votes
                        if list(tmp["field_type"])[0] in [
                            "ManyToOneRel",
                            "ManyToManyField",
                        ]:
                            related_model = mc[
                                (mc["table"] == rst_table)
                                & (mc["related_names"] == name)
                            ]
                            self.cols.append(list(related_model.field)[0] + "_id")
            except Exception as e:
                pass
        return rst_model, rst_table

    #
    # Try get the table name if the field name is unique in one of the table.
    #
    # @starter_point: str, The name of the first field. Check if the name string-matches the model name retreived from teh model_class table.
    # @return str,  the table name
    #
    # E.g., thumbnail  -> ('ThumbnailDimensions', 'easy_thumbnails_thumbnaildimensions')
    #
    def try_get_table_from_field(self, field_name, starter_point):
        model_class = self.model_class_info
        field_name_noid = field_name.replace("_id", "")
        model_with_field = model_class[
            (model_class["field"] == field_name)
            | (model_class["field"] == field_name_noid)
        ]

        if model_with_field.empty:
            return "", ""
        ##
        ## TODO. Not good to return the first result if multiple matches.
        ##
        if model_with_field.shape[0] > 1:
            if starter_point:
                nameb = starter_point.lower().replace("_", "").strip()
                possible_models = []
                for candidate_model in list(model_with_field.model):
                    namea = candidate_model.lower().replace("_", "").strip()
                    # Found the model with the same name
                    if (namea == nameb) or (namea in nameb) or (nameb in namea):
                        possible_models.append(candidate_model)

                if len(possible_models) > 0:
                    model_with_field = model_with_field[
                        model_with_field["model"].isin(possible_models)
                    ]

            if model_with_field.shape[0] > 1:
                self.extra = ", ".join(list(model_with_field.model))

        model_with_field.reset_index(drop=True, inplace=True)
        if self.class_name:
            tmpapp, tmpmodel = self.class_name.split(".")
            if tmpapp in list(model_with_field.app):
                model_with_field = model_with_field[model_with_field["app"] == tmpapp]

        # # If the field is m2m field, add the class_id to cols.
        # # The real column name may be different.
        if model_with_field.iloc[0]["foreign_type"]:
            rtable = model_with_field.iloc[0]["related_model"]
            ctable = model_with_field.iloc[0]["table"]
            tmp = list(
                model_class[
                    (model_class["table"] == rtable)
                    & (model_class["related_model"] == ctable)
                ].field
            )
            if len(tmp) >= 1:
                tmp_remote_field = tmp[0]
                self.cols.append(tmp_remote_field)

        focus_on_column_first = model_with_field[
            (model_with_field["field_type"] != "ManyToOneRel")
            & (model_with_field["field_type"] != "ManyToManyRel")
            & (model_with_field["field_type"] != "ManyToManyField")
        ]
        if not focus_on_column_first.empty:
            return (
                focus_on_column_first.iloc[0]["model"],
                focus_on_column_first.iloc[0]["table"],
            )

        return model_with_field.iloc[0]["model"], model_with_field.iloc[0]["table"]

    #
    # Try get the table name if the field name is unique in one of the table.
    #
    # @starter_point: str, The name of the first field. Check if the name string-matches the model name retreived from teh model_class table.
    # @return str,  the table name
    #
    # E.g., thumbnail  -> ('ThumbnailDimensions', 'easy_thumbnails_thumbnaildimensions')
    #
    def try_get_table_from_foreign_field(self, field_name, starter_point):
        model_class = self.model_class_info
        # model_class = model_class[model_class.foreign_type == 'ManyToManyField']
        model_with_field = model_class[
            (model_class.foreign_type == "ManyToManyField")
            & (model_class["field"] == field_name)
        ]

        if model_with_field.empty:
            return "", "", ""

        if model_with_field.shape[0] > 1:
            if starter_point:
                nameb = starter_point.lower().replace("_", "").strip()
                possible_models = []
                for candidate_model in list(model_with_field.model):
                    namea = candidate_model.lower().replace("_", "").strip()
                    # Found the model with the same name
                    if (namea == nameb) or (namea in nameb) or (nameb in namea):
                        possible_models.append(candidate_model)

                if len(possible_models) > 0:
                    model_with_field = model_with_field[
                        model_with_field["model"].isin(possible_models)
                    ]

            if model_with_field.shape[0] > 1:
                self.extra = ", ".join(list(model_with_field.model))

        r_model_table = list(model_with_field["related_model"])[0]
        try:
            r_model_model = list(
                model_class[model_class["table"] == r_model_table].model
            )[0]
        except:
            r_model_model = r_model_table.split("_")[1]
        return (
            list(model_with_field["model"])[0],
            r_model_model,
            list(model_with_field["table"])[0],
        )

    def try_get_table_from_attrs(self, attri_list, final=False):
        if attri_list == []:
            return None, None
        if len(attri_list) > 1:
            starter_point = self.get_attr_name_name(attri_list[-2])
        else:
            starter_point = None

        idx = len(attri_list)
        for node in attri_list[:0:-1]:
            idx -= 1
            try:
                field_name = self.get_attr_name_name(node)
                if field_name == "lower":
                    continue
                model, table = self.try_get_table_from_field(field_name, starter_point)
                starter_point = self.get_attr_name_name(node)

                if not final and (self.task_type == "FK" or self.task_type == "null"):
                    return model, table

                if model:
                    model_obj = self.get_modelobj_from_app_and_model("", model, table)
                    return self.get_table_from_list_with_starter(
                        attri_list[idx:], model_obj
                    )

            except Exception as e:
                print(traceback.format_exc())

        return None, None

    #
    # Get model for get_object_or_404 pattern
    # Two cases: 1) get_object_or_404(Model, field)
    # 2) get_object_or_404(obj,field) Need find the model for obj.Use def-use chain.
    #
    # @return str,  model_name, the table name
    #
    # E.g.,content_url = get_object_or_404(ContentUrl, token=token) -> ContentUrl
    #
    def get_object_or_404_get_model(self, node):
        try:
            model_name = node.args[0].id

            if model_name:
                # Case 1, the model_name is directly the Model name.
                subdf = self.model_class_info[
                    self.model_class_info["model"] == model_name
                ]
                if not subdf.empty:

                    if subdf.shape[0] == 1:
                        db_table_name = subdf.iloc[0]["table"]
                        return model_name, db_table_name

                    if self.class_name:
                        tmpapp, tmpmodel = self.class_name.split(".")
                        if tmpapp in list(subdf.app):
                            subdf = subdf[subdf["app"] == tmpapp]

                    db_table_name = subdf.iloc[0]["table"]
                    return model_name, db_table_name

                # Case 2, the model_name is a variable. Then try to find its definition model.
                model_node = node.args[0]
                model_name, db_table_name = self.find_obj_model_from_def_use(model_node)
                if model_name and db_table_name:
                    return model_name, db_table_name
            else:
                return "", ""

        except Exception as e:
            pass

        return "", ""

    #
    # Track the last class of the attr_list to get the table of unique
    #
    # @return str,  the class name
    #
    # E.g., user.addresses.add(address)
    #
    def track_M2M_attr_list(self, attri_list):
        try:
            node = attri_list[-2]
            name = self.get_attr_name_name(node)
            start_name = self.get_attr_name_name(attri_list[-3])
            if name in ["all", "filter"]:
                return None, None, None
            # The [-2] needs to be the foreign key / related name to another table -> unique together for these two models.
            (
                model_name,
                related_model_name,
                table_name,
            ) = self.try_get_table_from_foreign_field(name, start_name)

            return model_name, related_model_name, table_name
        except Exception as e:
            return "", "", ""

    #
    # Get the model from a list of strings
    #
    # @l, string of a list, e.g., model.objects.get  [Note] the sequece shoudl already be correct.
    #
    # @return str,  the model name, table_name
    #
    def get_model_from_list(self, l):
        try:
            for idx, item in enumerate(l):
                model_name = ""

                if self.get_attr_name_name(item) == "self":
                    app, model_name = self.class_name.split(".")

                if hasattr(l[idx], "attr") and (
                    l[idx].attr == "objects" or l[idx].attr == "_default_manager"
                ):
                    if hasattr(l[idx - 1], "id"):
                        model_name = l[idx - 1].id
                    elif hasattr(l[idx - 1], "attr"):
                        model_name = l[idx - 1].attr

                if model_name:
                    subdf = self.model_class_info[
                        self.model_class_info["model"] == model_name
                    ]
                    if not subdf.empty:
                        db_table_name = subdf.iloc[0]["table"]
                        return model_name, db_table_name

            try:
                if hasattr(l[0], "id"):
                    try_model_name = l[0].id
                elif hasattr(l[0], "attr"):
                    try_model_name = l[0].attr
                subdf = self.model_class_info[
                    self.model_class_info["model"] == try_model_name
                ]
                if not subdf.empty:
                    db_table_name = subdf.iloc[0]["table"]
                    return try_model_name, db_table_name
            except Exception as e:
                pass

            return "", ""
        except Exception as e:
            print("Exception in get_model_from_list, ", e, self.get_names(l))
            return "", ""

    #
    # Get model for obj, Use def-use chain.
    #
    # @obj_node: the AST node for the obj. Should be a Name node.
    # @return (str, str)  model_name, the table name
    #
    # E.g.,instrance = Model.objects.filter()    instance.xxx.xxx.get(...)  -> Know instrance is for Model.
    #
    # Update: Want to include another possible cases:
    # E.g. realm = Realm(xxx)  realm.xxx = xxx   -> Konw realm is for Class Realm.
    #
    def find_obj_model_from_def_use(self, obj_node):
        if self.defuse_num >= 1:
            return "", ""
        self.defuse_num += 1
        if obj_node in self.def_use_nodes:
            return "", ""
        self.def_use_nodes.append(obj_node)

        try:
            for def_ in self.udchains.chains[obj_node]:
                # def_ gives the defs. Need get its parent statement to do further things.
                # Should be: def is on the left side of the statement, and further check the right side.
                try:
                    parent = self.ancestors.parentStmt(def_.node)
                except:
                    continue

                attri_list = []
                # Get the attribute list.  `node.func` is the Attribute node.
                if not hasattr(parent, "value") or not hasattr(parent.value, "func"):
                    continue
                self.get_attr_list(parent.value.func, attri_list)

                # track_class_of_attr_list
                model_name, table_name = self.track_class_of_attr_list_for_null(
                    attri_list[::-1]
                )
                # Try get the filter columns in the kws for unique complex
                # E.g. emails = EmailAddress.objects.filter(email__iexact=value)
                #      if emails.filter(user=self.user).exists():
                # Then we want to add the email to the column list.
                try:
                    if self.task_type == "unique_complex":
                        cols = helper.get_column_lists(parent.value)
                        if cols:
                            self.cols += cols
                except:
                    pass

                if model_name and table_name:
                    return model_name, table_name
            return "", ""
        except Exception as e:
            return "", ""
        # TODO:No iteration here: If qs = func(qs1, qs_2), we don't try to get the type for qs1 for now.

    ###
    ### For NULL
    ###
    # From the model, check if the last item is a real column
    # Then add it to the cols
    def get_col_from_model(self, last_attr_name, class_name, table_name):
        # Double check if the last attri node is a column of the model.
        # If not,
        model_class = self.model_class_info
        model_with_field = model_class[
            (model_class["field"] == last_attr_name)
            & (model_class["model"] == class_name)
            & (model_class["table"] == table_name)
            & (model_class["field_type"] != "ManyToOneRel")
            & (model_class["field_type"] != "ManyToManyRel")
            & (model_class["field_type"] != "ManyToManyField")
        ]
        if model_with_field.shape[0] >= 1:
            cols = [last_attr_name]
        else:
            cols = []

        return cols

    # Match the pattern and add the attri_lists.
    def add_to_attrlist(self, var_node):
        # No need to deal with ast.Compare node as explictly dealt with.
        if isinstance(var_node, ast.Attribute):
            # Has the check pattern, then get the attris from the var_node
            attri_list = []
            self.get_attr_list(var_node, attri_list)
            names = self.get_names(attri_list[::-1])
            rst = {}
            rst["name"] = ".".join(names)
            rst["node"] = var_node

            self.checked_attributes.append(rst)
            return attri_list

    ###
    ### For FK
    ###
    def check_pk_in_detect_model(
        self, parent_class, parent_table, extra_models, models_with_pk
    ):
        if parent_class in models_with_pk:
            return parent_class, parent_table

        rst = []
        for model in models_with_pk:
            if model in extra_models:
                rst.append(model)
        if rst:
            self.extra = rst
            return "?", "?"
        else:
            return "", ""
