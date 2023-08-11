### Hi there 👋

<!--
**ryan-holthouse-lilly/ryan-holthouse-lilly** is a ✨ _special_ ✨ repository because its `README.md` (this file) appears on your GitHub profile.
-->

Attatched are two Python files.

audit_transform_ctwin.py:
Meant to take in audit trail files from the CT-WIN system, break them down to only T_ORDERS table values, then convert them into
activity tables. Data can be passed in by changing the filename at the top of the file. I originally had this taking in user 
input on runtime, but I changed this out for testing purposes. Output files are saved with the same naming conventions that
were supplied in the beginning.

audit_combination.py:
Takes in activity table (trail) and metadata files. They're then converted into dataframes once more and concatenated together.
Data is then run through last minute cleaning now that everything is all together. File outputs two files in Parquet format:
a full trail (activity table) file and a full metadata table for all orders in the set.
