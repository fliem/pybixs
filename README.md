# pybixs

pybixs is a package that allows to interface
[Openbis](https://wiki-bsse.ethz.ch/display/bis/Home)
(Open Source Biology Information System) via
[pybis](https://sissource.ethz.ch/sispub/pybis/) to interface a
database storing data from a
[BIDS](http://bids.neuroimaging.io)-formatted
(Brain Imaging Data Structure) study.

## DB schema
The following schema has to be implemented in Openbis.
The following terms are used interchangably by Openbis:
Collection = Experiment; Object = Sample

```
PROJECT
│-- SUBJECT_COLLECTION
│  │-- SUBJECT: subject_id="s1", sex="F"
│  │-- SUBJECT: subject_id="s2", sex="M"
│  │-- (...)
│
│-- MRI_SOURCEDATA_COLLECTION
│  │-- SESSION: subject_id="s1", session_id="tp1", session_type="MRI", parents=SUBJECT
│  │  │-- BIDS_NIFTI: subject_id="s1", session_id="tp1", bids_modality="T1w"
│  │  │-- BIDS_NIFTI: subject_id="s1", session_id="tp1", bids_modality="func"
│  │  │-- (...)
│  │
│  │-- SESSION: subject_id="s1", session_id="tp2", parents=SUBJECT
│  │  │-- (...)
│  │
│  │-- SESSION: subject_id="s2", session_id="tp2", parents=SUBJECT
│  │  │-- (...)
│  │
│  │-- (...)
│
│-- BEHAVIORAL_SESSION_COLLECTION
│  │-- SESSION: subject_id="s1", session_id="tp1", session_type="BEHAVIORAL", parents=SUBJECT
│
│-- BEHAVIORAL_SCORE_COLLECTION
│    │-- BEHAVIORAL_SCORE: subject_id="s1", session_id="tp1", parents=SESSION
│
│-- MRI_DERIVATE_COLLECTION (name="freesurver_v6")
│    │-- MRI_DERIVATE (level="group")
│       │-- BIDS_DERIVATE: subject_id="s1", parents=SUBJECT

```

### Collection/Experiment types
##### SUBJECT_COLLECTION
 * Stores data about subject that don't change across sessions (sex)
 * Sample of type SUBJECT is registered here

##### MRI_SOURCEDATA_COLLECTION
 * Stores data from mri session
 * Sample of type SESSION (session_type=”MRI”) is registered here (as child of SUBJECT)
 * Sample SESSION has data sets BIDS_NIFTI

##### BEHAVIORAL_SESSION_COLLECTION
  * Stores info about behavioral session
  * Sample of type SESSION (session_type=”BEHAVIORAL”) is registered here (as child of SUBJECT)

##### BEHAVIORAL_SCORE_COLLECTION
  * Stores values of behavioral tests
  * Sample of type BEHAVIORAL_SCORE is registered here (as child of SESSION)

##### MRI_DERIVATE_COLLECTION
  * Stores processed MRI Data
  * Mutliple collections can be registered and are distinguished by the name property
  * Sample of MRI_DERIVATE is registered here (which has BIDS_DERIVATE dataset)


### Sample/Object & Dataset types
Openbis does not have keys. The primary key is enforced at upload by
checking if object with primary key attributes is already registered.
In the tables below, keys are signified as follows:
*primary key*, *foreign key

If possible values of a property are given (in {}) the property is of type
CONTROLLEDVOCABULARY.

######
|Sample type SUBJECT        |
|---------  |
||
|*subject_id* |
|sex {F,M}  |
|comment    |


|Sample type SESSION       |
|---------  |
|registered as child of SUBJECT|
|*subject_id* *|
|*session_id*|
|*session_type* {MRI, BEHAVIORAL} |
|age|
|comment|


|Dataset type BIDS_NIFTI       |
|---------  |
|registered as dataset of SESSION|
|*subject_id* *|
|*session_id* *|
|*bids_modality*|
|*bids_type*|
|*bids_acquisition*|
|*bids_run*|
|*bids_task*|
|comment|


|Sample type BEHAVIORAL_SCORE       |
|---------  |
|registered as child of SESSION|
|*subject_id* *|
|*session_id* *|
|*behavioral_test* *|
|*score_name* *|
|score_value|
|conversion_date|
|comment|


|Sample type MRI_DERIVATE |
|---------|
|registered as child of SUBJECT|
|*name* *|
|*level* * {group, subject}|
|*subject_id* *|
|*session_id* *|
|comment|


