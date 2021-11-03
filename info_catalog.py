
import sys
import os
import logging
import sqlite3
import glob
import subprocess
import geomet
import json
import alphashape

DATA_VINTAGE="2021"

logger = logging.getLogger(__name__)

data_root = os.environ["HOME"] + f"/media/census_data/tiger/tiger_{DATA_VINTAGE}"

geos = ["13121","13089"]
## location = "tl_2020_21121"
tl_vint = f"tl_{DATA_VINTAGE}_"

datasets = ["EDGES", "ADDR", "ADDRFEAT", "ADDRFN", "FEATNAMES"]
us_datasets = ["ZCTA520", "COUNTY", "PLACE"]

WORK_DB = "build.db"
DEST_DB = "zip_lyr.db"

def get_geos(data_root):
    data = []
    geocode_start = len(data_root) + 15
    for g in glob.glob(f"{data_root}/EDGES/*.zip"):
        data.append(g[geocode_start:geocode_start+5])

    return data

def import_data_for_us(location, dbname=WORK_DB):

    if os.path.exists(dbname):
        os.remove(dbname)

    for f in us_datasets:
        loc = f"{data_root}/{f}/*.zip"
        for g in glob.glob(loc):
            dest_layer = f.lower()
            cmd = ['ogr2ogr', '-f', 'SQLITE', dbname, '-dsco', 'SPATIALITE=yes', f'/vsizip/{g}', '-nln', dest_layer, "-nlt", "PROMOTE_TO_MULTI"]

            #set non destruction flags if appending
            if os.path.exists(dbname):
                cmd.append("-append")
                cmd.append("-update")
            c = subprocess.call(cmd)

def import_data_for_geo(location, dbname=WORK_DB):

    if os.path.exists(dbname):
        os.remove(dbname)

    for f in datasets:
        loc = f"{data_root}/{f}/{location}*.zip"
        for g in glob.glob(loc):
            dest_layer = f.lower()
            cmd = ['ogr2ogr', '-f', 'SQLITE', dbname, '-dsco', 'SPATIALITE=yes', f'/vsizip/{g}', '-nln', dest_layer]

            #set non destruction flags if appending
            if os.path.exists(dbname):
                cmd.append("-append")
                cmd.append("-update")
            c = subprocess.call(cmd)


def build_zip_pts(geo_vint, geocode, dbname=WORK_DB):
    db = sqlite3.connect(dbname)
    db.enable_load_extension(True)
    db.execute('SELECT load_extension("mod_spatialite")')

    cur = db.cursor()

    cur.execute(f"""CREATE TABLE IF NOT EXISTS zip_pts (fid INTEGER PRIMARY KEY AUTOINCREMENT,
        zipcode TEXT,
        geocode TEXT,
        location TEXT -- one of fl, ml, tl, fr, mr, tr
        ) 
    """)

    # USE the NAD83 geographic system
    cur.execute("""SELECT addgeometrycolumn('zip_pts', 'geometry', 4269, 'POINT', 'XY')
    """)

    for offset in [["Left", "0.00005"], ["Right", "-0.00005"]]:
        for pos in [['From', 0.1],['Mid', 0.5],['To', 0.9]]:
            logging.info(f"constructing geometry for {geocode}: {pos[0]}-{offset[0]}")
            cur.execute(f"""INSERT INTO zip_pts (zipcode, geocode, location, geometry)
                SELECT zip, gc, loc, geom
                FROM
                (SELECT zipr as zip, '{geocode}' as gc, '{pos[0]}-{offset[0]}' as loc, 
                        st_line_interpolate_point(offsetcurve(geometry, {offset[1]}), {pos[1]}) as geom 
                    FROM addrfeat WHERE st_length(geometry) > 0.00003
                ) AS g
                WHERE g.zip is NOT NULL and
                    g.geom IS NOT NULL
            """)

    # wait for the build to complete
    assert cur.fetchall() is not None, "Failed to build zip_pts"

    cur.close()
    db.commit()

class ZIPWalker():
    db = None
    cur = None
    cache = []
    step = 0
    excludelist = []

    def __init__(self, db, geocode):
        self.cur = db.cursor()
        self.geocode = geocode

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.cache) == 0:
            self.update_cache()
            if len(self.cache) == 0:

                self.cur.close()
                raise StopIteration

        return self.cache.pop(0)

    def update_cache(self):
        #grab the next 5 zipcodes that have not yet been processed
        exclude_rowids = ",".join([d[1] for d in self.excludelist])
        self.cur.execute(f"""
        SELECT zipcode, asgeojson(collect(geometry)) 
            FROM zip_pts as p
            -- filter to objects not seen before
            WHERE NOT EXISTS (SELECT 1 FROM zip_polys z WHERE z.zipcode = p.zipcode) and
              zipcode not in ({exclude_rowids})
            GROUP BY 1 
            LIMIT 5
        """)

        self.cache = self.cur.fetchall()
        return self.cache

    def add_exception(self, exclude):
        self.excludelist.append(exclude)

def build_zip_polys(geo_vint, geocode, dbname=WORK_DB):
    db = sqlite3.connect(dbname)
    db.enable_load_extension(True)
    db.execute('SELECT load_extension("mod_spatialite")')

    cur = db.cursor()

    cur.execute("DROP TABLE IF EXISTS zip_polys")

    cur.execute(f"""CREATE TABLE IF NOT EXISTS zip_polys (fid INTEGER PRIMARY KEY AUTOINCREMENT,
        zipcode TEXT,
        geocode TEXT,
        location TEXT -- one of fl, ml, tl, fr, mr, tr
        ) 
    """)

    # USE the NAD83 geographic system
    cur.execute("""SELECT addgeometrycolumn('zip_polys', 'geometry', 4269, 'MULTIPOLYGON', 'XY')
    """)

    # set the initial alpha param to be ???
    alpha_compute_step = 0.8
    alpha_param = 225.0 / alpha_compute_step
    skipped = -1 

    # step through the zipcodes to limit memory usage
    # also reduce the alpha parameter as we go forward
    # until no records are able to be processed
    while alpha_param > 0.01 and not skipped == 0:
        alpha_param = alpha_param * alpha_compute_step
        logger.debug(f"attempt a build {geocode} ZIP Polygons with alpha parameter {alpha_param}")
        skipped = 0 

        # iterate through all of the zip codes
        zw = ZIPWalker(db, geocode)
        for item in zw:
            logger.debug(f"building polygon for {geocode}, ZIPCode {item[0]} at alpha {alpha_param}")

            # first pass try 'optimal' computed settings
            try:
                newpoly = alphashape.alphashape(json.loads(item[1]).get("coordinates"), alpha=alpha_param)
                irun_param = "auto"
            except ZeroDivisionError:
                skipped += 1
                zw.add_exception(item[0])
                # skip further processin of this zip
   
                continue
            
            newpts = newpoly.wkt
            #exclude Emtpy collections

            if newpoly is not None:
                if newpoly.geom_type in ("Polygon","MultiPolygon"):

                    cur.execute("""INSERT INTO zip_polys (zipcode, geometry) SELECT ?, st_multi(st_geometryfromtext(? , 4269))
                    """, (item[0], newpoly.wkt))
                    assert cur.fetchall() is not None, "errors when building polygons for ZIPCode {item[0]}"
                    logger.debug(f"built polygon for ZIPCode {item[0]} at alpha {alpha_param}")
            else:
                logger.debug(f"skipping built geometry of type {newpoly.geom_type}")
                zw.add_exception([geocode, item[0]])
                skipped += 1

            db.commit()

    cur.close()
    db.commit()

def build_zip_pts(geo_vint, geocode, dbname=WORK_DB):
    db = sqlite3.connect(dbname)
    db.enable_load_extension(True)
    db.execute('SELECT load_extension("mod_spatialite")')

    cur = db.cursor()

    cur.execute(f"""CREATE TABLE IF NOT EXISTS zip_pts (fid INTEGER PRIMARY KEY AUTOINCREMENT,
        zipcode TEXT,
        geocode TEXT,
        location TEXT -- one of fl, ml, tl, fr, mr, tr
        ) 
    """)

    # USE the NAD83 geographic system
    cur.execute("""SELECT addgeometrycolumn('zip_pts', 'geometry', 4269, 'POINT', 'XY')
    """)

    for offset in [["Left", "0.00005"], ["Right", "-0.00005"]]:
        for pos in [['From', 0.1],['Mid', 0.5],['To', 0.9]]:
            logging.info(f"constructing geometry for {geocode}: {pos[0]}-{offset[0]}")
            cur.execute(f"""INSERT INTO zip_pts (zipcode, geocode, location, geometry)
                SELECT zip, gc, loc, geom
                FROM
                (SELECT zipr as zip, '{geocode}' as gc, '{pos[0]}-{offset[0]}' as loc, 
                        st_line_interpolate_point(offsetcurve(geometry, {offset[1]}), {pos[1]}) as geom 
                    FROM addrfeat WHERE st_length(geometry) > 0.00003
                ) AS g
                WHERE g.zip is NOT NULL and
                    g.geom IS NOT NULL
            """)

    # wait for the build to complete
    assert cur.fetchall() is not None, "Failed to build zip_pts"

    cur.close()
    db.commit()


def read_geom(data):
    wkt = data
    return wkt

def main(args):
    os.environ["OGR_SQLITE_SYNCHRONOUS"] = "OFF"
    #get_geos(data_root)

    # import_data_for_us(f"{tl_vint}", dbname=f"build_us.db")

    for geo in geos:
        logger.info(f"building ZIP PTS for {geo}")
        # import_data_for_geo(f"{tl_vint}{geo}", dbname=f"build_{geo}.db")
        # build_zip_pts(f"{tl_vint}{geo}", geocode=geo, dbname=f"build_{geo}.db")
        build_zip_polys(f"{tl_vint}{geo}", geocode=geo, dbname=f"build_{geo}.db")

    logger.info("All Done")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)

