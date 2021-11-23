library(aws.s3)
library(dplyr)
library(sf)

AWS_S3_RAW_BUCKET <- Sys.getenv("AWS_S3_RAW_BUCKET")
current_year <- strftime(Sys.Date(), "%Y")

# CHICAGO COMMUNITY AREA
remote_file_community_area <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "community_area",
  paste0(current_year, ".zip")
)
tmp_file_community_area <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_community_area)) {
  st_read(paste0(
    "https://data.cityofchicago.org/api/geospatial/",
    "cauq-8yn6?method=export&format=GeoJSON"
  )) %>%
    st_write(tmp_file_community_area, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_community_area, remote_file_community_area)
  file.remove(tmp_file_community_area)
}

# UNINCORPORATED AREA
remote_file_unincorporated_area <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "unincorporated_area",
  paste0("2014", ".zip")
)
tmp_file_unincorporated_area <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_unincorporated_area)) {
  st_read(paste0(
    "https://datacatalog.cookcountyil.gov/api/geospatial/",
    "kbr6-dyec?method=export&format=GeoJSON"
  )) %>%
    st_write(tmp_file_unincorporated_area, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_unincorporated_area, remote_file_unincorporated_area)
  file.remove(tmp_file_unincorporated_area)
}

# INDUSTRIAL GROWTH ZONE
remote_file_industrial_growth_zone <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "industrial_growth_zone",
  paste0("2019", ".geojson")
)
tmp_file_industrial_growth_zone <- tempfile(fileext = ".geojson")

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_industrial_growth_zone)) {
  st_read(paste0(
    "https://opendata.arcgis.com/datasets/",
    "76e52da12b56406c945662eea968f3e1_1.geojson"
  )) %>%
    st_write(tmp_file_industrial_growth_zone, delete_dsn = TRUE)
  aws.s3::put_object(tmp_file_industrial_growth_zone, remote_file_industrial_growth_zone)
  file.remove(tmp_file_industrial_growth_zone)
}

# BUILDING FOOTPRINTS
remote_file_footprints <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other",
  "building_footprint", paste0(current_year, ".geojson"))

if (!aws.s3::object_exists(remote_file_footprints)) {

  temp_file <- tempfile(fileext = ".geojson")

  # Gather building footprints from OSM
  footprints <- opq("Cook County United States") %>%
    add_osm_feature(key = "building") %>%
    osmdata_sf()

  # Only need polygons
  footprints <- footprints$osm_polygons

  # Transform
  footprints %>%
    st_transform(crs = 3435) %>%
    st_make_valid() %>%

    # Intersect with Cook County border
    st_intersection(
      st_transform(
        st_read(
          "https://opendata.arcgis.com/datasets/ea127f9e96b74677892722069c984198_1.geojson"
        ),
        3435
      )
    ) %>%
    pull(geometry) %>%

    # Upload to AWS
    st_write(temp_file)

  aws.s3::put_object(temp_file, remote_file_footprints, multipart = TRUE)
  file.remove(temp_file)

}

# QUALIFIED OPPORTUNITY ZONE
remote_file_qualified_opportuniy_zone <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "qualified_opportuniy_zone",
  paste0("2019", ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_qualified_opportuniy_zone)) {

  tmp1 <- tempfile(fileext = ".zip")
  tmp2 <- tempfile()
  tmp3 <- tempfile(fileext = ".geojson")

  # Grab zipped shapefile - https://www.cdfifund.gov/opportunity-zones
  download.file(
    "https://www.cdfifund.gov/sites/cdfi/files/documents/opportunity-zones=8764.-9-10-2019.zip",
    destfile = tmp1
  )

  # Unzip shapefile
  unzip(tmp1, exdir = tmp2, overwrite = T)

  # Only keep Cook County, save as geojson
  st_read(grep("shp", list.files(tmp2, recursive = TRUE, full.names = TRUE), value = T)) %>%
    filter(STATENAME == 'Illinois' & COUNTYNAME == 'Cook') %>%
    st_write(tmp3)

  # Upload, clean
  aws.s3::put_object(tmp3, remote_file_qualified_opportuniy_zone)
  file.remove(c(tmp1, tmp3, list.files(tmp2, full.names = TRUE)))

}

# SUBDIVISIONS
remote_file_subdivision <- file.path(
  AWS_S3_RAW_BUCKET, "spatial", "other", "subdivision",
  paste0(current_year, ".geojson")
)

# Write file to S3 if it doesn't already exist
if (!aws.s3::object_exists(remote_file_subdivision)) {

  aws.s3::put_object("O:/CCAODATA/data/spatial/Subdivisions.geojson",
                     remote_file_subdivision,
                     multipart = TRUE)

}

# Cleanup
rm(list = ls())