from pybixs.bids import upload_bids_mri_study, upload_mri_derivate, upload_bids_behavioral_study

space = "LHAB"

project = "LHAB23"
demographics_file = 'sourcedata/participants.tsv'
sourcedata_dir = "sourcedata"
behavdata_dir = "new_behav"
derivates_dir = "derivates"

subject_ids = ["s1", "s2"]
session_ids = None

upload_bids_mri_study(space, project, sourcedata_dir, behavdata_dir, demographics_file, subject_ids=subject_ids,
                      session_ids=session_ids)

behav_demographics_file = demographics_file
upload_bids_behavioral_study(space, project, behavdata_dir, behav_demographics_file, subject_ids=subject_ids,
                             session_ids=session_ids)

upload_mri_derivate(space, project, derivates_dir, "freesurfer")
