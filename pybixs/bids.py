from .openbisio import open_connection, get_collection_identifiers, register_object, get_objects, get_object_permid, \
    get_permId_mapping, wait_until_upload_registered, check_permids_available
import os
import pandas as pd
import json
import time
from glob import glob
from bids.grabbids import BIDSLayout
import re

NII_EXT = ".nii.gz"


##### UTILS
###########

def get_bids_collection_identifiers(session, space, project):
    collection_identifiers = get_collection_identifiers(session, space, project,
                                                        collection_type=["SUBJECT_COLLECTION",
                                                                         "MRI_SOURCEDATA_COLLECTION",
                                                                         "BEHAVIORAL_SESSION_COLLECTION",
                                                                         "BEHAVIORAL_SCORE_COLLECTION"])
    return collection_identifiers


def get_derivates_collection_identifiers(session, space, project):
    try:
        experiments = session.get_experiments(space=space, project=project, type="MRI_DERIVATE_COLLECTION")
    except:
        experiments = []
    collection_identifiers = {}
    for e in experiments:
        if e.props.name in collection_identifiers.keys():
            raise Exception("Name {} duplicated {}".format(e.props.name, collection_identifiers))
        collection_identifiers[e.props.name] = e.permId

    return collection_identifiers


def setup_project(session, space, project):
    # creates project and BIDS specific collections
    # 1. Project
    p = session.new_project(space=space, code=project, description="BIDS project")
    p.save()
    # 2. Collections
    for c in ["SUBJECT_COLLECTION", "MRI_SOURCEDATA_COLLECTION", "BEHAVIORAL_SESSION_COLLECTION",
              "BEHAVIORAL_SCORE_COLLECTION"]:
        coll = session.new_experiment(type=c, code=c, project=project)
        coll.save()
    wait_until_upload_registered(session, coll.permId, kind="experiment")


def prepare_demos(demographics_file, level, subject_ids=None, session_ids=None, ):
    """
    splits demo df into age (1 val per subject) and age (1 val per session)

    sex_df has subject_id as index
    age_df has (subject_id, session_id) as index

    Returns df (selected by level {"subject", "session"})
    """
    df = pd.read_csv(demographics_file, sep="\t")
    df.rename(columns={"participant_id": "subject_id"}, inplace=True)

    if subject_ids:
        df = df[df.subject_id.isin(subject_ids)]
    if session_ids:
        df = df[df.session_id.isin(session_ids)]

    if level == "subject":
        sex_df = df[["subject_id", "sex"]].drop_duplicates()
        sex_df.set_index("subject_id", inplace=True, verify_integrity=True)
        return sex_df
    elif level == "session":
        age_df = df[["subject_id", "session_id", "age"]].drop_duplicates()
        age_df.set_index(["subject_id", "session_id"], inplace=True, verify_integrity=True)
        return age_df
    else:
        raise Exception("Level  {} not known.".format(level))


def prepare_bids_df(sourcedata_dir, subject_ids=None, session_ids=None):
    layout = BIDSLayout(sourcedata_dir)
    bids_df = layout.as_data_frame()
    bids_df.dropna(subset=["subject"], inplace=True)  # remove study level info
    nii_df = bids_df[bids_df.path.str.endswith(NII_EXT)]
    nii_df = nii_df.sort_values(by=["subject", "session", "modality", "type", "run"])

    # reduce to requested sessions
    if subject_ids:
        nii_df = nii_df[nii_df.subject.isin(subject_ids)]
    if session_ids:
        nii_df = nii_df[nii_df.session.isin(session_ids)]
    return nii_df


def prepare_behav_df(behavdata_dir, subject_ids=None, session_ids=None):
    """
    loads *_long.tsv from behavdata_dir
    """
    suff = "_long.tsv"
    df = pd.DataFrame([])
    files = glob(os.path.join(behavdata_dir, "*" + suff))
    for f in files:
        df_ = pd.read_csv(f, sep="\t")
        df_["behavioral_test"] = "_".join(os.path.basename(f).split(suff)[0].split("_")[1:])
        df = df.append(df_)
    df.drop(labels="file", axis=1, inplace=True)

    # reduce to requested sessions
    if subject_ids:
        df = df[df.subject_id.isin(subject_ids)]
    if session_ids:
        df = df[df.session_id.isin(session_ids)]
    return df


def reduce_session_df(session_df, data_df, data_source):
    """
    retains only sessions in session_df that have kind=[brain/behavioural] data at session
    """

    if data_source == "brain":
        data = data_df.rename(columns={"subject": "subject_id", "session": "session_id"})
    else:
        data = data_df.copy()
    data = data[["subject_id", "session_id"]].drop_duplicates()
    data["data_available"] = True
    data.set_index(["subject_id", "session_id"], inplace=True, verify_integrity=True)

    session_df = session_df.join(data, how="right")
    session_df.drop("data_available", axis=1, inplace=True)
    return session_df


def get_one_df_line_as_dict(line):
    """ go via json to make sure too high precision of to_dict does not mess data up

    e.g.
    df = pd.DataFrame({"a":[1,2], "b":[1.0,2.2], "c":["hihi", "haha"]})
    In [73]: df.iloc[0].to_json()
    Out[73]: '{"a":1,"b":1.1,"c":"hihi"}'
    In [74]: df.iloc[0].to_dict()
    Out[74]: {'a': 1, 'b': 1.1000000000000001, 'c': 'hihi'}
    """
    if not isinstance(line, pd.core.series.Series):
        raise Exception("Something seems wrong. This function takes one line of a data frame (as series). {"
                        "}".format(line))
    d = json.loads(line.to_json())
    return d


def register_derivate_collection(session, project, name):
    print("Creating MRI_DERIVATE_COLLECTION {}".format(name))
    t = "MRI_DERIVATE_COLLECTION"
    e = session.new_experiment(project=project, type=t, code=t + "_" + name, props={"name": name})
    e.save()
    wait_until_upload_registered(session, e.permId, kind="experiment")


def register_subject(session, space, project, subject_id, df):
    """
    registers subject_id and sex in SUBJECT_COLLECTION
    """
    collection_identifiers = get_bids_collection_identifiers(session, space, project)

    props = {"subject_id": subject_id}
    props.update(get_one_df_line_as_dict(df.loc[subject_id]))

    permId = register_object(session, space, project,
                             experiment=collection_identifiers["SUBJECT_COLLECTION"],
                             object_type="SUBJECT",
                             props=props, primary_key=["subject_id"],
                             kind="sample", fast_mode=True)
    return permId


def register_session(session, space, project, subject_id, session_id, df, subject_mapping, collection_mapping,
                     session_type):
    collection_name_mapping = {"MRI": "MRI_SOURCEDATA_COLLECTION",
                               "BEHAVIORAL": "BEHAVIORAL_SESSION_COLLECTION"}
    experiment = collection_mapping[collection_name_mapping[session_type]]

    # get parent subject
    subject_permid = subject_mapping[subject_id]

    props = {"subject_id": subject_id,
             "session_id": session_id,
             "session_type": session_type}
    props.update(get_one_df_line_as_dict(df.loc[subject_id, session_id]))  # age

    permId = register_object(session, space, project,
                             experiment=experiment,
                             object_type="SESSION",
                             props=props, primary_key=["subject_id", "session_id"],
                             kind="sample", parents=[subject_permid], fast_mode=True)
    return permId


def create_subject(session, space, project, subject_df):
    """
    creates subject sample in SUBJECT_COLLECTION for all subjects in subject_df
    """
    check_permIds = []

    for subject_id in subject_df.index.unique():
        permId = register_subject(session, space, project, subject_id, subject_df)
        check_permIds.append(permId)
    check_permids_available(session, check_permIds)


def create_session(session, space, project, session_df, session_type):
    """
    all sessions in session_df are created
    index of df is (subject_id, session_id)
    """
    collection_mapping = get_bids_collection_identifiers(session, space, project)
    subject_mapping = get_permId_mapping(session, project,
                                         props={"experiment": "SUBJECT_COLLECTION", "type": "SUBJECT"},
                                         index_cols=["subject_id"])

    check_permIds = []
    for i in range(len(session_df)):
        # get subject_id and session_id from index
        subject_id, session_id = session_df.iloc[i].name
        permId = register_session(session, space, project, subject_id, session_id, session_df, subject_mapping,
                                  collection_mapping, session_type)
        check_permIds.append(permId)
    check_permids_available(session, check_permIds)


def upload_mri_data(session, space, project, sourcedata_dir, nii_df):
    session_mapping = get_permId_mapping(session, project,
                                         props={"experiment": "MRI_SOURCEDATA_COLLECTION", "type": "SESSION"},
                                         index_cols=["subject_id", "session_id"])

    check_permIds = []
    for i in range(len(nii_df)):
        nii_props = get_one_df_line_as_dict(nii_df.iloc[i])
        permId = register_mri_data(session, space, project, sourcedata_dir, NII_EXT, session_mapping, nii_props)
        check_permIds.append(permId)
    check_permids_available(session, check_permIds, kind="dataset")


def register_mri_data(session, space, project, sourcedata_dir, nii_ext, session_mapping, nii_props):
    """
    takes dict with nii_props (from pybids data frame) and uploads dataset
    """
    collection_identifiers = get_bids_collection_identifiers(session, space, project)
    experiment = collection_identifiers["MRI_SOURCEDATA_COLLECTION"]

    # openbis requires relative paths
    rel_path_starting_point = os.path.join(sourcedata_dir, "..")
    os.chdir(rel_path_starting_point)
    path_pref = os.path.relpath(nii_props["path"].split(nii_ext)[0])
    files = glob(path_pref + "*")

    # get session permid
    session_permid = session_mapping[(nii_props["subject"], nii_props["session"])]
    # upload data
    props = {
        "subject_id": nii_props["subject"],
        "session_id": nii_props["session"],
        "bids_modality": nii_props["modality"],
        "bids_type": nii_props["type"],
        "bids_acquisition": nii_props["acquisition"],
        "bids_run": nii_props["run"],
        "bids_task": nii_props["task"],
    }
    permId = register_object(session, space, project,
                             experiment=experiment,
                             object_type="BIDS_NIFTI",
                             props=props,
                             primary_key=["subject_id", "session_id", "bids_modality", "bids_type", "bids_acquisition",
                                          "bids_run", "bids_task"],
                             # test permid alt
                             kind="dataset", sample=session_permid,
                             files=files, fast_mode=True)
    return permId


def upload_behav_data(session, space, project, behav_df):
    session_mapping = get_permId_mapping(session, project,
                                         props={"experiment": "BEHAVIORAL_SESSION_COLLECTION", "type": "SESSION"},
                                         index_cols=["subject_id", "session_id"])

    check_permIds = []
    for i in range(len(behav_df)):
        behav_props = get_one_df_line_as_dict(behav_df.iloc[i])
        permId = register_behav_data(session, space, project, session_mapping, behav_props)
        check_permIds.append(permId)
    check_permids_available(session, check_permIds)


def register_behav_data(session, space, project, session_mapping, behav_props):
    collection_identifiers = get_bids_collection_identifiers(session, space, project)

    # get session permid
    session_permid = session_mapping[(behav_props["subject_id"], behav_props["session_id"])]

    # DONE: fixme consider changing in behav export: variable->score_name, value->score_value;
    #  then upload_prop=behav_props TEST
    # upload_props = {"subject_id": behav_props["subject_id"],
    #                 "session_id": behav_props["session_id"],
    #                 "behavioral_test": behav_props["behavioral_test"],
    #                 "score_name": behav_props["variable"],
    #                 "score_value": behav_props["value"],
    #                 "conversion_date": behav_props["conversion_date"],
    #                 }
    upload_props = behav_props
    permId = register_object(session, space, project,
                             experiment=collection_identifiers["BEHAVIORAL_SCORE_COLLECTION"],
                             object_type="BEHAVIORAL_SCORE",
                             props=upload_props,
                             primary_key=["subject_id", "session_id", "behavioral_test", "score_name"],
                             # test permid alt
                             kind="sample",
                             parents=[session_permid], fast_mode=True)
    return permId


def upload_bids_behavioral_study(space, project, behavdata_dir, behav_demographics_file, subject_ids=None,
                                 session_ids=None):
    # fixme username
    session = open_connection(username="admin")

    ## behav session
    # fixme get real behav sessions
    behav_session_df = prepare_demos(behav_demographics_file, level="session", subject_ids=subject_ids,
                                     session_ids=session_ids)
    behav_df = prepare_behav_df(behavdata_dir, subject_ids=subject_ids, session_ids=session_ids)
    ### reduce to only include sessions with behav data available
    behav_session_df = reduce_session_df(behav_session_df, data_df=behav_df, data_source="behavior")

    # register BEHAVIORAL session
    print("**** create behavioral sessions")
    t1 = time.time()
    create_session(session, space, project, behav_session_df, session_type="BEHAVIORAL")

    t2 = time.time()
    print("**** upload behavioral data")
    upload_behav_data(session, space, project, behav_df)
    t3 = time.time()

    print("behav session registration", t2 - t1)
    print("behav data upload", t3 - t2)

    session.logout()


def upload_bids_mri_study(space, project, sourcedata_dir, behavdata_dir, demographics_file, subject_ids=None,
                          session_ids=None):
    # fixme username
    session = open_connection(username="admin")

    try:
        session.get_project(project)
    except:
        setup_project(session, space, project)

    # get dfs
    ## subject
    subject_df = prepare_demos(demographics_file, level="subject", subject_ids=subject_ids, session_ids=session_ids)

    ## mri session
    mri_session_df = prepare_demos(demographics_file, level="session", subject_ids=subject_ids,
                                   session_ids=session_ids)
    bids_df = prepare_bids_df(sourcedata_dir, subject_ids=subject_ids, session_ids=session_ids)
    ### reduce to only include sessions with mri available
    mri_session_df = reduce_session_df(mri_session_df, data_df=bids_df, data_source="brain")

    print("**** create subjects")
    t1 = time.time()
    create_subject(session, space, project, subject_df)

    t2 = time.time()
    # register MRI session
    print("**** create mri sessions")
    create_session(session, space, project, mri_session_df, session_type="MRI")

    t3 = time.time()
    upload_mri_data(session, space, project, sourcedata_dir, bids_df)

    t4 = time.time()

    print("subject registration", t2 - t1)
    print("session registration", t3 - t2)
    print("mri upload", t4 - t3)

    session.logout()


def upload_mri_derivate(space, project, derivates_dir, name,
                        subject_level_patterns=["sub-{subject_id}*"],
                        group_level_patterns=["*group*"],
                        subject_regex=r"sub-([\w]+)_", subject_ids=None):
    session = open_connection(username="admin")

    #  get identifiers or setup Collection
    derivates_collection_identifiers = get_derivates_collection_identifiers(session, space, project)
    if name not in derivates_collection_identifiers.keys():
        register_derivate_collection(session, project, name)
    derivates_collection_identifiers = get_derivates_collection_identifiers(session, space, project)

    register_mri_derivate(session, space, project, derivates_dir, name,
                          derivates_collection_identifiers=derivates_collection_identifiers,
                          subject_level_patterns=subject_level_patterns, group_level_patterns=group_level_patterns,
                          subject_regex=subject_regex, subject_ids=subject_ids)


def upload_mri_derivate_group(session, space, project, name, path_pref, derivates_collection_identifiers,
                              group_level_patterns):
    # Group data
    group_data = []
    for p in group_level_patterns:
        group_data += glob(os.path.join(path_pref, p))
    upload_props = {"name": name,
                    "level": "GROUP"}
    # Register derivate sample
    sample_permId = register_object(session, space, project,
                                    experiment=derivates_collection_identifiers[name],
                                    object_type="MRI_DERIVATE",
                                    props=upload_props,
                                    primary_key="all",
                                    kind="sample",
                                    )
    # Register derivate dataset
    register_object(session, space, project,
                    experiment=derivates_collection_identifiers[name],
                    object_type="BIDS_DERIVATE",
                    props={},
                    # test permid alt
                    kind="dataset", sample=sample_permId,
                    files=group_data)


def upload_mri_derivate_subjects(session, space, project, name, subject_ids, path_pref,
                                 derivates_collection_identifiers, subject_mapping, subject_level_patterns,
                                 subject_regex):
    # Subject data
    if not subject_ids:
        # get a list of subjects
        subject_objects = []
        subject_ids = []
        get_all_subjects_pattern = [s.replace("{subject_id}", "") for s in subject_level_patterns]

        # get an initial list with all subjects
        for p in get_all_subjects_pattern:
            subject_objects += glob(os.path.join(path_pref, p))
        # filter subject_ids
        for s in subject_objects:
            match = re.search(subject_regex, s)
            g = match.groups()
            if len(g) == 0:
                raise Exception("No subject pattern found {}".format(s))
            elif len(g) > 1:
                raise Exception("Multiple subject patterns found {}.".format(s))
            else:
                subject_ids.append(g[0])
        subject_ids = list(set(subject_ids))

    # list subject data
    # fixme permid check in the end
    check_permIds = []
    for subject_id in subject_ids:
        subject_data = []
        for p in subject_level_patterns:
            subject_data += glob(os.path.join(path_pref, p.format(subject_id=subject_id)))

        # upload data
        upload_props = {"name": name,
                        "level": "SUBJECT"}
        sample_permId = register_object(session, space, project,
                                        experiment=derivates_collection_identifiers[name],
                                        object_type="MRI_DERIVATE",
                                        props=upload_props,
                                        primary_key="all",
                                        kind="sample",
                                        parents=subject_mapping[subject_id])
        permId = register_object(session, space, project,
                                 experiment=derivates_collection_identifiers[name],
                                 object_type="BIDS_DERIVATE",
                                 props={},
                                 kind="dataset", sample=sample_permId,
                                 files=subject_data, fast_mode=True)
        check_permIds.append(permId)
    check_permids_available(session, check_permIds, kind="dataset")


def register_mri_derivate(session, space, project, derivates_dir, name, derivates_collection_identifiers,
                          subject_level_patterns, group_level_patterns, subject_regex, subject_ids=None):
    subject_mapping = get_permId_mapping(session, project,
                                         props={"experiment": "SUBJECT_COLLECTION", "type": "SUBJECT"},
                                         index_cols=["subject_id"])

    os.chdir(os.path.join(derivates_dir, ".."))
    path_pref = os.path.relpath(os.path.join(derivates_dir, name))

    upload_mri_derivate_group(session, space, project, name, path_pref, derivates_collection_identifiers,
                              group_level_patterns)

    upload_mri_derivate_subjects(session, space, project, name, subject_ids, path_pref,
                                 derivates_collection_identifiers, subject_mapping, subject_level_patterns,
                                 subject_regex)
