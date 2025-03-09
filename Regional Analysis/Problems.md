# Challenges
## Integrate big data analysis with non-big data tools
We want to do some geospatial analysis, such as interacting with OSM, spatial join two locations, plot maps, etc. Unfortunately, many popular geospatial analysis tools are not big data tools (for example, Geopy, GeoPandas, Osmnx, etc.). Those tools are well integrated with Pandas so making geospatial analysis with Pandas very convenient and easy. 

There indeed is one big data tool that can handle large size of spatial data, which is the [Apache Sedona](https://sedona.apache.org/latest/setup/overview/) (previously called GeoSpark). However, the documentation of Sedona is not that friendly, and it costs a lot of time making the environment and dependencies work.

But wait, are we really working with large geospatial data? Think about the scenario where we want to use geospatial data. It is only when we want to find the store locations, or plot store distribution, or find nearby amenities near stores. It is all about stores. So how many stores do we get? Check the original data and we found, there are only 3354 stores in total, which means, the geospatial data we want to use is at most 3354 rows. We can tolerate that with Pandas and other Pandas-friendly tools.

So our strategy is quite straightforward, we use Spark to “shrink” the data into a minimum number of rows before we feed it to Pandas, do whatever geospatial operation with this smaller table, save the result and convert back to Spark, and use Spark to join back this result with the original big data for whatever further analysis. Which means, we only use Pandas when we have to, and we always check the number of rows we send to Pandas.

This strategy can be reflected in the implementations of [POI analysis](Topic2.md#count-number-of-pois-within-2km-for-each-liquor-store) and [filling missing location data](../Data_Processing_Cleaning/DataCleaning_README.md#add-locations-from-openstreetmap-osm).

## Messy data from OpenStreetMap
OpenStreetMap (OSM) is a collaborative, open-source mapping platform where anyone can contribute and use detailed geographic data freely for various applications. It is good in its rich contents and API services, and it is totally free. However, because anyone can annotate, the data on OSM is sometimes messy. Here are some situations we encountered and how we dealt with it:
### When searching locations based on store name or store address.
In the [data cleaning process](../Data_Processing_Cleaning/DataCleaning_README.md#add-locations-from-openstreetmap-osm), we try to search for missing locations (longitude & latitude coordinates) using geopy from OSM data. By using only the store name, we got extremely vague results, especially for chain stores that share the same name (with different numbers appending the store name)  in the same city. Because not all stores are annotated on OSM, so OSM will just return a closest match it got, probably the same name but in a completely different city or even different state (when we don’t limit to Iowa). We cannot trust these data, so that was why we simply dropped Step 3 in the data cleaning process.

Searching locations based on store address is much better, especially when we add the city and state info to limit its searching range. However, sometimes the matched result is not the building (the liquor store), but a highway, a line. In this case, the geocoding won’t return valid coordinates.

We checked the Google Map, the store names on Google Map are much more accurate than OSM, and giving addresses will return very accurate coordinates. Besides the geocoding service by Google Map allows batch querying, which will greatly speed up the searching speed. The only problem is Google Map isn’t free, and that was why we did not use it.

### Looking for tags that are relevant but cannot find any result
When we tried to [find local places of interest near liquor stores](Topic2.md#search-places-of-interests-poi-by-openstreetmap), one problem we encountered was to determine the tag to use. There are so many [tags](https://wiki.openstreetmap.org/wiki/Map_features) in OSM, there are “tags” under tags, which ones shall we look for? 

At first we thought all relevant tags were under the “amenity” type, such as `“amenity”: “restaurant”`, `“amenity”:”supermarket”`, etc. 
Then after searching those tags by osmnx, it returned 0 supermarket and 0 stadium. We thought it would be impossible to have no stadium at all in Polk County! So we tried searching [“stadium near Polk County”](https://www.openstreetmap.org/search?query=stadium%20near%20Polk%20County#map=19/41.740010/-93.603568) directly on OpenStreetMap webpage. By examining the result, we found that the correct tag should be `“leisure”: “stadium”`.

So finally the tags we used for searching local POIs became this:
```python
  tags = {
      "amenity": ["bar", "pub", "restaurant", "cafe"],
      "leisure": ["stadium"],
      "shop": ["supermarket"]
  }
```
### Repetitive annotation of the same building
When [counting the number of POIs within 2km of a liquor store](Topic2.md#count-number-of-pois-within-2km-for-each-liquor-store), we found for some liquor store, the number of nearby stadium is 12. However it is impossible to have 12 stadiums in a 2km range! Again we checked the store and stadium on OSM website, and found out that [this stadium](https://www.openstreetmap.org/search?query=stadium%20near%20302%20S%20ANKENY%20BLVD#map=19/41.740010/-93.603568) has been repetitively annotated for 12 times (Perhaps peaple were annotating the different seat areas). In this case, we use our human reasoning, to limit the number of stadium to be one whenever a match has been found.

## Normalize data for fair comparision
This one is just a small tips when we tried to compare sales among different stores. We noticed that it is not fair to compare the total sales or total transactions, since some shop has opened for several years but other stores might just opened for a few months. It also does not help if divide the sales with the same time span (say for example 10 years of our dataset). The method we use is to find the earlist and latest record a store has, divide the difference by 365 days and round to a whole year. Now the normalization is store-wise, depending on the number of years it has opened.
