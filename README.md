# uscb_zipbuilder

Construct a geographic area for ZIP codes based on TIGER edges, features, and addresses

## plan of operation
* For a single TIGER vintage
  * download the National ZCTA5 file  
* for a single state and county
  * build edges, addr, addrfeat tables
  * build points for the endpoints and midpoint of the addressed with ZIP Codes
  * Run an alphashape generator on each point set

