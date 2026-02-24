print('custom function to process geospatial data vis pandas / geopandas')
def waa_load_geospatial_csv(csv_location:str, out_name:str, lakehouse:str = 'WaterAbstractioneAlert') ->None:
    #--------------------------------
    # Save down path output locations
    #--------------------------------
    geojson_path = f'/lakehouse/default/Files/Rainfall/Pete_McColl/{out_name}'
    spark_path = f'{lakehouse}.dbo.{out_name}'
    #===============================
    # Begin Procedure
    #===============================
    try:
        pd.options.mode.string_storage = "python" # needs to be forced, as on arrow, it causes geo data to fail it seems  "input object not numpy error"
        #---------------------------
        # 1. Read CSV file and clean up before processing
        #---------------------------
        print(f'1️⃣📖INFO: Reading in {csv_location.split("/")[-1]},for processing\n')
        csv_file = pd.read_csv(filepath_or_buffer=csv_location)
        csv_file_cleaned = clean_columns(csv_file)
        csv_file_cleaned["geometry"] = csv_file_cleaned["geometry"].apply(wkt.loads) # Parse WKT geometry - need to know how this is done (geopandas?)
        #---------------------------
        # 2. Process into Geopandas Dataframe format
        #---------------------------
        print(f'2️⃣📏INFO: Converting into Geopandas Dataframe\n')
        csv_file_geo = gpd.GeoDataFrame(csv_file_cleaned, geometry='geometry', crs = 'EPSG:4326') 
        
        #===========================
        # 3. Saving Data into GeoJson and Spark table Formats
        #===========================
        print(f'''3️⃣📏INFO: Saving Data into GeoJson and Spark table Formats
                  \n🗺️⚠️GeoJSON NOTE: Geometry is preserved as is
                  \n💻⚠️Spark NOTE: Geometry column needs converting to text for preservation,else wont parse (Arrow conflicts in Spark)\n''')

        #---------------------------
        # 3.1 Saving as GeoJSON
        #---------------------------
        print(f'INFO: 🎚️🗺️ Processing {csv_location.split("/")[-1]} into GeoJSON file.\n')
        csv_file_geo.to_file(f'{geojson_path}.geojson', driver = 'GeoJSON')
        print(f'INFO: ✅🗺️ Processed Data {csv_location.split("/")[-1]} saved in {geojson_path} as a geojson file.\n')

        #---------------------------
        # 3.2 Saving as Spark Table in Lakehouse
        #---------------------------
        print(f'INFO: 🎚️💻 Processing {csv_location.split("/")[-1]} into Spark Lakehouse Table.\n')
        csv_file_geo_t = csv_file_geo.copy()
        csv_file_geo_t["geometry_wkt"] = csv_file_geo_t["geometry"].apply(lambda geom: geom.wkt) # Retain geometry info, but in a way spark won't hate :)
        csv_file_geo_t = csv_file_geo_t.drop(columns = ['geometry'])                             # Remove original geometry (still in GeoJSON version)
        csv_file_spark = spark.createDataFrame(csv_file_geo_t)                                 # Create spark dataframe  before impute into lakehouse
        csv_file_spark.write.option("overWriteSchema",True).saveAsTable(spark_path, mode = 'overwrite',format = 'delta')
        print(f'INFO: ✅💻 Processed Data {csv_location.split("/")[-1]} saved in {spark_path} as a Lakehouse Spark table.\n')
        #===========================
        # 4. Confirm Processing is complete and plot data
        #===========================
        print(f'COMPLETE: 🏁 Processed data is now finished !\n')
        csv_file_geo.info()
        csv_file_geo.plot()
        csv_file_geo_t.plot()

    except Exception as e:
        print(f'❌ERROR: Something went wrong {e}')
