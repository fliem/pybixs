import os
from glob import glob
from pybis import Openbis
import getpass
import time
import pandas as pd
import json


def open_connection(username, url):
    "creates connection to openbis and returns session object"
    s = Openbis(url=url, verify_certificates=True)
    if not username:
        username = input("Enter OpenBis Username:")

    password = getpass.getpass(prompt="Enter password for {}: ".format(username))

    s.login(username, password)
    assert s.token is not None
    assert s.is_token_valid() is True
    return s


def get_collection_identifiers(session, space, project, collection_type):
    """
    returns identifiers for collections
    """
    identifiers = {}
    for t in collection_type:
        experiments = session.get_experiments(space=space, project=project, type=t)
        if len(experiments) == 1:
            identifiers[t] = experiments[0].identifier
        elif len(experiments) == 0:
            raise Exception("Collection not found {}\nSetup a project with the setup_project function".format(
                collection_type))
        elif len(experiments) > 1:
            raise Exception("Multiple collections found {}".format(collection_type))
    return identifiers


def get_objects(session, kind, props, raise_if_no_objects_found=False):
    """
    queries the database for objects (kind = sample or dataset) with props
    """
    fu = {"sample": session.get_samples, "dataset": session.get_datasets, "experiment": session.get_experiments}

    # get_datasets doesnt take permId, but only code
    if kind == "dataset" and "permId" in props.keys():
        props["code"] = props.pop("permId")

    # if dataset and space in props, remove (get_datasets doesn't take space)
    if kind == "dataset" and "space" in props.keys():
        props.pop("space")

    try:
        objects = fu[kind](**props)
    except ValueError as e:
        if (e.args[0] == 'no samples found!') | (e.args[0] == 'no datasets found!'):
            objects = []
        else:
            raise
    if raise_if_no_objects_found and len(objects) == 0:
        raise Exception("No objects found and raise_if_no_objects_found is True. {} {}".format(kind, props))
    return objects


def get_object_permid(objects, raise_if_multiple=False, raise_if_none=True):
    permIds = []
    for o in objects:
        permIds.append(o.permId)

    if len(permIds) == 1:
        permIds = permIds[0]
    elif len(permIds) > 1:
        if raise_if_multiple:
            raise Exception("Multiple objects found, but raise_if_multiple set to True {}.\n".format(objects))
    else:
        if raise_if_none:
            raise Exception("No objects found, but raise_if_none set to True {}.\n".format(objects))

    return permIds


def look_for_experiment(session, permId):
    experiments = session.get_experiments()
    df = experiments.df
    return df.loc[df.permId.values == permId]


def check_permids_available(session, permIds, kind="sample", verbose_after_sec=2, wait_inc_sec=.1):
    """
    checks if a list of permIds are available in cache
    """
    for permId in permIds:
        wait_until_upload_registered(session, permId, kind=kind, verbose_after_sec=verbose_after_sec,
                                     wait_inc_sec=wait_inc_sec)


def wait_until_upload_registered(session, permId, kind="sample", verbose_after_sec=10, wait_inc_sec=.1):
    """ this function can be used to check for a permId directly after an upload
     to avoid issues with the cache delay, wait until sample can be found """
    found = False
    t = 0
    while not found:
        if kind == "experiment":
            # expermints are different in pybis so extrawurst
            objects = look_for_experiment(session, permId)
        else:
            objects = get_objects(session, kind, {"permId": permId})
        if len(objects) == 0:
            p = "%s not found. wait..." % permId
            time.sleep(wait_inc_sec)
        else:
            found = True
            p = "%s found. OK!" % permId
        t += wait_inc_sec
        if t > verbose_after_sec:
            print(p)


def get_all_properties_df(session, project, props={}):
    """ returns a data frame with all samples and datasets in a project and their properties """

    def _get_relations(kind):
        get_one_func = {"sample": session.get_sample, "dataset": session.get_dataset}
        df_props = pd.DataFrame([])
        df_relations = pd.DataFrame([])

        query_props = props
        query_props.update({"project": project})
        objects = get_objects(session, kind, query_props)
        if objects == []:
            df_objects = pd.DataFrame([])
        else:
            df_objects = objects.df

        for i in range(len(df_objects)):
            one_object = get_one_func[kind](df_objects.loc[i, "permId"])
            df_props_ = pd.DataFrame(one_object.props.all(), index=[i])
            df_props = pd.concat((df_props, df_props_), axis=0)
            df_ = pd.DataFrame({"children": [one_object.children], "parents": [one_object.parents]}, index=[i])
            df_relations = pd.concat((df_relations, df_), axis=0)
        df_combined = pd.concat((df_objects, df_props, df_relations), axis=1)
        return df_combined

    df_samples = _get_relations("sample")
    df_datasets = _get_relations("dataset")
    df = pd.concat((df_samples, df_datasets), axis=0, ignore_index=True)
    return df


def get_selected_properties_df(session, project, props, index_cols, out_cols):
    """
    lists all objecst in database with props
    retains only columns given in out_cols and sets index_cols as index
    e.g. index_cols: ["subject_id", "session_id"], out_col ["permid"]
    returns
                                              permId
    subject_id session_id
    s1         tp1         XXX
               tp2         XXX
    """
    df = get_all_properties_df(session, project, props)
    df.set_index(index_cols, inplace=True, verify_integrity=True)
    return df[out_cols]


def get_properties_mapping_from_df(df, mapping_var=None):
    """
    takes a data frame and returns mapping dict, e.g.

    df:
                                              permId
    subject_id session_id
    s1         tp1         XXX
               tp2         XXX


    returns
    {'permId': {('s1', 'tp1'): 'XXX',
      ('s1', 'tp2'): 'XXX'}}

    if called with mapping_var=str; selects key; e.g. mapping_var=permId
    returns
    {('s1', 'tp1'): 'XXX',
        ('s1', 'tp2'): 'XXX'}
    """
    d = df.to_dict()
    if mapping_var:
        d = d[mapping_var]
    return d


def get_permId_mapping(session, project, props, index_cols):
    """
    queries
    returns mapping dict; e.g.

    get_permId_mapping(session, project, props={"experiment": "SUBJECT_COLLECTION"}, index_cols=["subject_id"])
    returns
        {'s1': 'XXX', 's2': 'XXX'}
    """
    df = get_selected_properties_df(session, project, props, index_cols, ["permId"])
    d = get_properties_mapping_from_df(df, mapping_var="permId")
    return d


# move to bids module
def check_duplicates(session, project):
    print("Checking for duplicates")
    df = get_all_properties_df(session, project)

    c = ['identifier', 'permId', 'experiment', 'registrator',
         'registrationDate', 'modifier', 'modificationDate', 'children', 'parents', 'properties', 'sample', 'location']
    df.drop(c, axis=1, inplace=True)
    if "Comment" in df.columns:
        df.drop(["Comment"], axis=1, inplace=True)

    dup_ix = df.duplicated()
    if dup_ix.any():
        print(df[dup_ix])
        raise Exception("%d intries in the data base are duplicates!!!" % dup_ix.sum())
    else:
        print("No duplicates found. OK.")


def register_object(session, space, project, experiment, object_type, props, primary_key="all", kind="sample",
                    sample=None, files=None, parents=None, fast_mode=False):
    """
    register a object (sample or dataset)

    primary_key: determines if db should be scanned for objects with the same properties
     - if: "all": all props are considered primary keys
     - if list: list objects are pk
     - if None: don't check for duplicates
    kind: sample or dataset
    sample & files are only relevant for dataset upload
    files to upload to dataset (directories will be walked)
    fast_mode does not wait until permId is available from the db (only do that if you check for permIds at the end
    of a loop)

    returns permid of the registered (or found) object
    """
    verbose_after_sec = 10
    wait_inc_sec = .1

    # files can include directories that need to be walked to list all files in directory
    upload_files = []
    if files:
        for f in files:
            if os.path.isdir(f):
                for path, subdirs, filenames in os.walk(f):
                    for filename in filenames:
                        upload_files.append(os.path.join(path, filename))
            else:
                upload_files.append(f)
        upload_files = list(set(upload_files))
        upload_files.sort()

    # clean props: remove entries with None (openbis does not like them)
    props = {k: v for k, v in props.items() if v}

    # prepare relevant upload arguments
    upload_args = {"space": space,
                   "project": project,
                   "experiment": experiment,
                   "type": object_type,
                   "props": props
                   }
    if kind == "dataset":
        upload_args["sample"] = sample
        upload_args["files"] = upload_files
        verbose_after_sec = 15
        wait_inc_sec = 1
    if parents:
        upload_args["parents"] = parents

    # if primary key is defined as list or "all", check if the sample has already been registered
    if primary_key:
        if primary_key == "all":
            primary_key = props.keys()
        # only allow primary keys that don't have None value in props:
        primary_key = [pk for pk in primary_key if pk in props.keys()]

        primary_key_dict = dict(zip(primary_key, [props[k] for k in primary_key]))
        primary_key_dict.update({"space": space, "project": project, "experiment": experiment, "type": object_type})
        if kind == "dataset":
            primary_key_dict["sample"] = sample
        objects = get_objects(session, kind, primary_key_dict)

        if len(objects) == 1:
            # object extists
            upload_object = False
            permid = get_object_permid(objects)
            print("{} already present. do nothing: {} pk: {}\n ".format(kind, upload_args.items(), primary_key))
        elif len(objects) > 1:
            raise Exception("{} present more than once: {}, {},\n {}".format(kind, len(objects), upload_args.items(),
                                                                             objects))
        else:
            upload_object = True

    else:
        upload_object = True

    if upload_object:
        fu = {"sample": session.new_sample, "dataset": session.new_dataset}
        try:
            obj = fu[kind](**upload_args)
            obj.save()
            permid = obj.permId
            if not fast_mode:
                wait_until_upload_registered(session, permid, kind, verbose_after_sec=verbose_after_sec,
                                             wait_inc_sec=wait_inc_sec)
            print("{} sucessfully uploaded: {} pk: {}\n".format(kind, upload_args.items(), primary_key))
        except Exception as e:
            print("something went wrong: {}\n".format(upload_args.items()))
            print(e)
            raise

    return permid


def type_dump(out_dir):
    """
    exports all types
    """
    # fixme name
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)
    session = open_connection(username="admin")
    fu = {"sample_type": session.get_sample_types, "dataset_type": session.get_dataset_types,
          "experiment_type": session.get_experiment_types}
    for t in fu.keys():
        object_types = fu[t]()
        for o in object_types:
            name = o.code
            print(name)
            with open(os.path.join(out_dir, "{}_{}.txt".format(t, name)), "w") as fi:
                json.dump(o.data, fi, indent=4)

    t = "vocabulary"
    o = session.get_terms()
    with open(os.path.join(out_dir, "{}.txt".format(t)), "w") as fi:
        json.dump(o.data, fi, indent=4)