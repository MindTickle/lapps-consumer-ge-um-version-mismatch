# lapps-consumer-version-mismatch
Identify and correct version mismatch data entries in GE and UM for existing users

Fetch ids of all the records from UM in batch and push in kafka
Write Consumer to do this:
For those records check with GE version if version mismatch:
1. Case UM < GE, then Upgrade UM
2. Case UM > GE, then call GE to update the version (in case version upgrade fails then downgrade UM)