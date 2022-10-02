import gast as ast, ast as old_ast
import sys
import traceback
from . import helper
from . import basic_traverse

if sys.version_info.major >= 3:
    from importlib import reload
reload(helper)
reload(basic_traverse)


#######################
## Get Keyword pattern for constraints. Such as:
## Voucher.objects.get(id=self.voucher_id) ,  self -> OrderDiscount.
## Parent can be get from the another model.
#######################
class FKFinderGet(basic_traverse.AttrAnalysis):

    GET_KEYWORDS = ["get", "get_or_create", "update_or_create", "filter"]
    GET_KEYWORDS_ID = ["get_object_or_404", "get_object_for_this_type"]

    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(FKFinderGet, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "FK"

    #
    # Call nodes for pattern1: attr_list.get(col)
    #
    def visit_Call(self, node):
        try:
            # Clean it every time. Or the provious will polute it.
            self.cols = []
            self.extra = ""

            get_type = self._is_get(node)
            if get_type:
                # If keyword is pk or id, no need to record this.
                if hasattr(node, "keywords") and len(node.keywords) == 0:
                    return

                # Filter one type: self.get(reverse...)
                if self.filter_some_get_usage(node):
                    return

                # Get the attribute list on the left.  `node.func` is the Attribute node.
                attri_list = []
                self.get_attr_list(node.func, attri_list)
                names = self.get_names(attri_list[::-1])

                # Get the parent table
                if get_type == "get":
                    parent_class, parent_table = self.track_class_of_attr_list(
                        attri_list[::-1]
                    )
                # 'get_object_or_404', model is at args[0]
                elif get_type == "get_obj":
                    parent_class, parent_table = self.get_object_or_404_get_model(node)
                else:
                    return

                # Check if the cols contain the primary key in the parents table.
                tmp = self.model_class_info[
                    self.model_class_info["table"] == parent_table
                ]
                if tmp.empty:
                    self.generic_visit(node)
                    return
                # Get the name of the primary key.
                pk_name = tmp[tmp["primary_key"] == "True"].iloc[0]["field"]
                # Get the list of column = val on the right hand side of the table.get(col1=, col2=)
                # The returned node is the 'value' node of k-v.
                pk_key, parent_node = helper.get_column_list_with_node_for_fk(
                    node, pk_name
                )
                if not pk_key:
                    return
                self.cols.append(pk_key)

                # For FK, find the child class from the rhs_vals
                child_class, child_table, child_list, child_col = self.get_rhs_value(
                    parent_node
                )

                if (not parent_class) or (not parent_table) or (not child_class):
                    self.attribute_list.append(
                        ".".join(names) + "." + ",".join(self.cols)
                    )
                else:
                    filtered = False
                    if not child_table:
                        filtered = True

                    self.extract_constraints.append(
                        {
                            "referenced_class": parent_class,
                            "referenced_table": parent_table,
                            "referenced_col": ",".join(self.cols),
                            "referenced_usage": ".".join(names),
                            "lineno": str(self.init_lineno + node.lineno),
                            "source": "get_type",
                            "file": self.filepath,
                            "dependent_class": child_class,
                            "dependent_table": child_table,
                            "dependent_col": child_col,
                            "dependent_usage": ".".join(child_list),
                            "extra": self.extra,
                            "filtered": filtered,
                        }
                    )
        except Exception as e:
            pass
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
            if (node.func.attr in FKFinderGet.GET_KEYWORDS) and len(node.keywords) > 0:
                return "get"

        except Exception as e:
            pass

        # ['get_object_or_404', "get_object_for_this_type"]
        try:
            if (
                hasattr(node.func, "id") and node.func.id in FKFinderGet.GET_KEYWORDS_ID
            ) or (
                hasattr(node.func, "attr")
                and node.func.attr in FKFinderGet.GET_KEYWORDS_ID
            ):
                return "get_obj"
        except Exception as e:
            pass

        return None

    #
    # Get the attributes on rhs. Goal is to see if they are from another model.
    #
    # @parent_node, value of primary keywords, e.g., (id=self.voucher_id)
    #
    def get_rhs_value(self, parent_node):
        try:
            attri_list = []
            self.get_attr_list(parent_node, attri_list)

            if not attri_list:
                return None, None, None, None
            child_class, child_table = self.track_class_of_attr_list(attri_list[::-1])
            if not child_class:
                child_class = self.get_names(attri_list[::-1])

            child_list = self.get_names(attri_list[::-1])
            if child_list:
                child_col = child_list[-1]
            else:
                child_col = None

            # Check if col exist in co-exist.
            child_col_list = [child_col, child_col.replace("_id", "")]
            coexist = self.model_class_info[
                (self.model_class_info["table"] == child_table)
                & (self.model_class_info["field"].isin(child_col_list))
            ]
            if coexist.empty:
                return None, None, None, None
            # If col is PK of the model, filter this. (PK cannot be FK)
            elif list(coexist["primary_key"])[0] == "True":
                return None, None, None, None

            return child_class, child_table, child_list, child_col

        except Exception as e:
            print(
                "get_rhs_value Exception, ",
                traceback.format_exc(),
                "keyword, ",
            )
            return None, None, None, None


#
# Another pattern is  Assign
#
# ModelA.col = ModelB.pk
#
# E.g., order_discount.voucher_id = voucher.id
#
class FKFinderAssignPK(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(FKFinderAssignPK, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "FK"

    #
    # Assign nodes for pattern: send_request.stream.first_message_id = send_request.message.id
    #
    def visit_Assign(self, node):
        # Clean it every time. Or the provious will polute it.
        self.cols = []
        self.extra = ""

        # Only care about the model.pk/id
        rst, subinfo = self.check_attribute_pk(node.value)
        # print("190: ", rst, subinfo, str(self.init_lineno + node.lineno))

        if rst == False:
            return

        # Get the parent model and table.
        parent_attri_list = []
        # Another case is on the right only one variable. Need find its def-use to determine.
        if rst == 2:
            parent_attri_list.append(subinfo)
            parent_pk = "id"
        # Make sure the right is a model.pk
        elif rst == 1:
            parent_pk = subinfo
            self.get_attr_list(node.value, parent_attri_list)

        if not parent_attri_list:
            return
        parent_names = self.get_names(parent_attri_list[::-1])
        parent_usage = parent_names
        self.defuse_num = 0
        parent_class, parent_table = self.track_class_of_attr_list(
            parent_attri_list[::-1]
        )
        if not parent_class:
            return
        # Check if generated model uses that primary key.
        potential_model_rows = self.model_class_info[
            (self.model_class_info["primary_key"] == "True")
            & (self.model_class_info["field"] == parent_pk)
        ]
        if parent_class and parent_pk not in ["id", "pk"]:
            parent_class, parent_table = self.check_pk_in_detect_model(
                parent_class,
                parent_table,
                self.extra,
                list(potential_model_rows["model"]),
            )
        if not parent_class:
            return
        # Get the attribute list on the left.  `node.targets[0]` is the Attribute node.
        # E.g., order_discount.voucher_id = voucher.id,  we want to get the model and the col.
        attri_list = []
        self.get_attr_list(node.targets[0], attri_list)
        if not attri_list:
            return

        names = self.get_names(attri_list[::-1])
        child_class, child_table = self.track_class_of_attr_list(attri_list[::-1])
        child_col = names[-1]

        if (not child_class) or (not child_table) or (not child_col):
            self.attribute_list.append(".".join(names) + "." + ",".join(self.cols))
        else:
            if child_col == "id" or child_col == "pk":
                return
            self.extract_constraints.append(
                {
                    "referenced_class": parent_class,
                    "referenced_table": parent_table,
                    "referenced_col": parent_pk,
                    "referenced_usage": ".".join(parent_usage),
                    "lineno": str(self.init_lineno + node.lineno),
                    "source": "AssignPK",
                    "file": self.filepath,
                    "dependent_class": child_class,
                    "dependent_table": child_table,
                    "dependent_col": child_col,
                    "dependent_usage": ".".join(names),
                    "extra": self.extra,
                    "filtered": False,
                }
            )

        self.generic_visit(node)

    #
    # Check if this could be related to a PK.
    # @return, True/False, with the
    #
    def check_attribute_pk(self, value_node):
        pk_names = list(
            set(
                self.model_class_info[
                    self.model_class_info["primary_key"].isin(["True", "TRUE"])
                ].field
            )
        )
        if hasattr(value_node, "attr"):
            return value_node.attr in pk_names, value_node.attr
        elif isinstance(value_node, ast.Name):
            if value_node.id == "self":
                return 2, value_node
            try:
                for def_ in self.udchains.chains[value_node]:
                    # def_ gives the defs. Need get its parent statement to do further things.
                    # Should be: def is on the left side of the statement, and further check the right side.
                    try:
                        parent = self.ancestors.parentStmt(def_.node)
                    except:
                        continue
                    # Get the attribute list.  `node.func` is the Attribute node.
                    if not hasattr(parent, "value") or not hasattr(
                        parent.value, "func"
                    ):
                        continue
                    # Require the attr be a model, not some random functions.
                    first_attr = parent.value.func.attr
                    tmp = self.model_class_info[
                        self.model_class_info["model"] == first_attr
                    ]
                    if (not tmp.empty) or (first_attr in ["create"]):
                        return 2, value_node
                return False, None
            except Exception as e:
                return False, None
        else:
            return False, None


#
# Another pattern is  (keyword.value)
#
# ModelA.objects.filter(col = modelB.id/pk)
# ModelA(col = modelB.id/pk).save()
#
# When check, I only make sure
# E.g., order_discount = OrderDiscount(offer_id=discount['offer'].id)
#
class FKFinderKeyValuePK(basic_traverse.AttrAnalysis):
    def __init__(
        self, class_name, udchains, ancestors, init_lineno, model_class_info, filepath
    ):
        super(FKFinderKeyValuePK, self).__init__(
            class_name, udchains, ancestors, init_lineno, model_class_info, filepath
        )
        self.task_type = "FK"

    #
    # Assign nodes for pattern: send_request.stream.first_message_id = send_request.message.id
    #
    def visit_Call(self, node):
        # Clean it every time. Or the provious will polute it.
        self.cols = []
        self.extra = ""

        # Only care about the modelb.pk/id
        child_class, child_table, child_usage = self.check_left_model(node)

        for kwnode in node.keywords:
            # offer_id=self.id
            child_col = kwnode.arg
            # If the column is the PK, or column not in the table lists, or the column is already a FK, then ignore this
            if not self.check_field_in_dependeent_table(child_class, child_col):
                continue

            if isinstance(kwnode.value, ast.Attribute) or isinstance(
                kwnode.value, ast.Name
            ):
                last_node = self.get_attr_name_name(kwnode.value)
                # The rhs need to be the primary key.
                potential_model_rows = self.model_class_info[
                    (self.model_class_info["primary_key"] == "True")
                    & (self.model_class_info["field"] == last_node)
                ]

                if not potential_model_rows.empty:
                    attri_list = []
                    self.get_attr_list(kwnode.value, attri_list)
                    parent_names = self.get_names(attri_list[::-1])
                    if not attri_list:
                        continue
                    parent_class, parent_table = self.track_class_of_attr_list(
                        attri_list[::-1]
                    )
                    # If the potential_model_rows with that PK not appear in potential tables in self.extra, ...
                    if parent_class and last_node not in ["id", "pk"]:
                        parent_class, parent_table = self.check_pk_in_detect_model(
                            parent_class,
                            parent_table,
                            self.extra,
                            list(potential_model_rows["model"]),
                        )
                    if last_node in ["id", "pk"] and len(self.extra.split(",")) > 7:
                        parent_class, parent_table = "?", "?"
                        self.extra = "Too many"

                    filtered = False
                    if not parent_class:
                        filtered = True

                    self.extract_constraints.append(
                        {
                            "referenced_class": parent_class,
                            "referenced_table": parent_table,
                            "referenced_col": last_node,
                            "referenced_usage": ".".join(parent_names),
                            "lineno": str(self.init_lineno + node.lineno),
                            "source": "KeyValuePK",
                            "file": self.filepath,
                            "dependent_class": child_class,
                            "dependent_table": child_table,
                            "dependent_col": child_col,
                            "dependent_usage": child_usage,
                            "extra": self.extra,
                            "filtered": filtered,
                        }
                    )

        self.generic_visit(node)

    def check_field_in_dependeent_table(self, child_class, child_col):
        if not child_col or not child_class:
            return False

        child_col = child_col.replace("__in", "")
        # If the column is the PK,
        if (child_col in ["id", "pk"]) or ("__" in child_col):
            return False

        subdf = self.model_class_info[
            (self.model_class_info["model"] == child_class)
            & (self.model_class_info["field"] == child_col)
        ]
        # If the column is already a FK
        if (not subdf.empty) and list(subdf.field_type)[0] == "ForeignKey":
            return False

        # If column not in the table lists
        subdf2 = self.model_class_info[self.model_class_info["field"] == child_col]
        if subdf2.empty:
            return False

        return True

    # Must be a model here on the left side.
    #  OrderDiscount(.. )
    #
    def check_left_model(self, node):
        if isinstance(node.func, ast.Name) and isinstance(node.func.ctx, ast.Load):
            model_name = node.func.id
            subdf = self.model_class_info[self.model_class_info["model"] == model_name]
            if not subdf.empty:
                db_table_name = subdf.iloc[0]["table"]
                return model_name, db_table_name, model_name
            else:
                return None, None, None
        elif isinstance(node.func, ast.Attribute):
            attri_list = []
            self.get_attr_list(node.func, attri_list)
            names = self.get_names(attri_list[::-1])
            if not attri_list:
                return None, None, None

            names = self.get_names(attri_list[::-1])
            child_class, child_table = self.track_class_of_attr_list(attri_list[::-1])

            return child_class, child_table, ".".join(names)
        return None, None, None
