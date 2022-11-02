from . import beniget
from . import basic_traverse
from . import unique_traverse
from . import notnull_traverse
from . import fk_traverse
import sys
from . import helper

if sys.version_info.major >= 3:
    from importlib import reload
reload(helper)
reload(basic_traverse)
reload(unique_traverse)
reload(notnull_traverse)
reload(fk_traverse)

#
# Return use-def chain and ancestors
#
def beniget_analysis(module):
    du = beniget.DefUseChains()
    du.visit(module)
    udchains = beniget.UseDefChains(du)
    ancestors = beniget.Ancestors()
    ancestors.visit(module)
    return udchains, ancestors


###########################
# Not null constraints.
###########################
def pattern_finder(
    filepath, extract_constraints, init_usage, model_class_info, pattern
):

    # Get the class sub tree
    module, source = helper.get_module_from_path(filepath)
    class_trees = basic_traverse.get_class_tree(module)

    basic_traverse.assign_parents_to_ast(module)

    # GET udchains
    try:
        udchains, ancestors = beniget_analysis(module)
    except Exception as e:
        import traceback

        print("46 udchain failed: ", traceback.format_exc())

    path_prefix = "/app_code/"
    if "history_issues" in filepath:
        path_prefix = "/history_issues/"
        
    dir_idx = filepath.find(path_prefix)
    subfilepath = filepath[dir_idx + len(path_prefix) :]

    # Iterate the classes
    for class_tree in class_trees:
        module = class_tree["node"]

        class_name = ""
        if class_tree["type"] == "class":
            class_name = basic_traverse.gen_class_name(
                module.name, filepath.split("/")[-2]
            )

        # Main process
        if pattern == "unique":
            constraint_patterns = [
                unique_traverse.UniqueFinderGet,
                unique_traverse.UniqueFinderM2M,
                unique_traverse.UniqueFinderCheckThenAction,
            ]
        elif pattern == "fk":
            constraint_patterns = [
                fk_traverse.FKFinderGet,
                fk_traverse.FKFinderAssignPK,
                fk_traverse.FKFinderKeyValuePK,
            ]
        elif pattern == "null":
            constraint_patterns = [
                notnull_traverse.NotNullFinder,
                notnull_traverse.NotNullCheckExecptionPattern,
                notnull_traverse.NullablePattern,
            ]

        for pattern_handler in constraint_patterns:
            attr = pattern_handler(
                class_name, udchains, ancestors, 0, model_class_info, subfilepath
            )
            attr.visit(module)

            # Aggregate the results
            init_usage += list(attr.attribute_list)
            extract_constraints += list(attr.extract_constraints)
