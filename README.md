# ouroboros_assets
Assets for Ouroboros

## Folders
* **bag_classes**
 * This folder is meant to house python classes used during ingest to create bags.  These classes are dropped in during the `ingestWorkspace()` workflow, after a job has been created, the metadata parsed, and the intellectual objects inserted into MySQL.  These classes might be applicable across collections, but might also be collection specific.