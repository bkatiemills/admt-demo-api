use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Serialize, Deserialize};
use serde_json::json;
use mongodb::{Client, options::ClientOptions};
use futures::stream::StreamExt;
use std::env;
use std::collections::HashMap;
use mongodb::bson::{self, Bson, Document};
use mongodb::options::FindOptions;
use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::cmp::Ordering;

static CLIENT: Lazy<Mutex<Option<mongodb::Client>>> = Lazy::new(|| Mutex::new(None));

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GeoJSONPoint {
    #[serde(rename = "type")]
    location_type: String,
    coordinates: [f64; 2],
} 

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataInfo {
    DATA_MODE: String,
    UNITS: String,
    LONG_NAME: String,
    PROFILE_PARAMETER_QC: String,
} 

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DataSchema {
    _id: String,
    geolocation: GeoJSONPoint,
    CYCLE_NUMBER: i32,
    DIRECTION: String,
    DATA_STATE_INDICATOR: String,
    DATA_MODE: String,
    DATE_CREATION: String,
    DATE_UPDATE: String,
    DC_REFERENCE: String,
    JULD: f64,
    JULD_QC: String,
    JULD_LOCATION: f64,
    POSITION_QC: String,
    VERTICAL_SAMPLING_SCHEME: String,
    CONFIG_MISSION_NUMBER: i32,
    STATION_PARAMETERS: Vec<String>,
    realtime_data: Option<HashMap<String, Vec<f64>>>,
    adjusted_data: Option<HashMap<String, Vec<f64>>>,
    data_info: Option<HashMap<String, DataInfo>>,
    level_qc: Option<HashMap<String, Vec<String>>>,
    adjusted_level_qc: Option<HashMap<String, Vec<String>>>,
    source_file: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct MapSchema {
    _id: String,
    geolocation: GeoJSONPoint,
    JULD: f64,
    STATION_PARAMETERS: Vec<String>,
    source_file: String,
}

#[get("/query_params")]
async fn get_query_params(query_params: web::Query<serde_json::Value>) -> impl Responder {
    let params = query_params.into_inner();
    HttpResponse::Ok().json(params)
}

#[get("/search")]
async fn search_data_schema(query_params: web::Query<serde_json::Value>) -> impl Responder {
    let page: u64 = query_params.get("page").map(|d| d.as_str().unwrap().parse::<u64>().unwrap_or(0)).unwrap_or(0);
    let page_size: i64 = 1000;

    // Extract the query parameters
    let polygon = query_params.get("polygon").map(|p| p.as_str().unwrap());
    let startDate = query_params.get("startDate").map(|d| d.as_str().unwrap().parse::<f64>().unwrap());
    let endDate = query_params.get("endDate").map(|d| d.as_str().unwrap().parse::<f64>().unwrap());
    let id = query_params.get("id").map(|i| i.as_str().unwrap());
    let mode = query_params.get("mode")
        .map(|m| m.as_str().unwrap())
        .filter(|&m| m == "map")
        .unwrap_or("full"); 

    let mut data_map: HashMap<String, Vec<i32>> = HashMap::new();
    let mut current_key: Option<String> = None;
    if let Some(data_str) = query_params.get("data").map(|d| d.as_str().unwrap()) {
        for piece in data_str.split(',') {
            match piece.parse::<i32>() {
                Ok(num) => {
                    if let Some(key) = &current_key {
                        data_map.entry(key.clone()).or_insert_with(Vec::new).push(num);
                    }
                }
                Err(_) => {
                    current_key = Some(piece.to_string());
                    data_map.entry(piece.to_string()).or_insert_with(Vec::new);
                }
            }
        }
    }
    let data: Vec<String> = data_map.keys().cloned().collect();

    let pres_range: Vec<f64> = query_params.get("presRange")
        .map(|p| p.as_str().unwrap().split(',').map(|s| s.parse::<f64>().unwrap()).collect())
        .unwrap_or(Vec::new());


    // Build the filter based on the provided parameters
    let mut filter = mongodb::bson::doc! {};

    let mut has_polygon = false;
    if let Some(polygon) = polygon {
        has_polygon = true;
        let polygon_coordinates: Vec<Vec<Vec<f64>>> = serde_json::from_str(polygon).unwrap();
        let polygon_geojson = bson::to_bson(&json!({
            "type": "Polygon",
            "coordinates": polygon_coordinates
        })).unwrap();
        filter.insert("geolocation", mongodb::bson::doc! { "$geoWithin": { "$geometry": polygon_geojson } });
    }

    let mut has_startDate = false;
    let mut has_endDate = false;
    if let (Some(startDate), Some(endDate)) = (startDate, endDate) {
        has_startDate = true;
        has_endDate = true;
        filter.insert("JULD", mongodb::bson::doc! { "$gte": startDate, "$lt": endDate });
    } else if let Some(startDate) = startDate {
        has_startDate = true;
        filter.insert("JULD", mongodb::bson::doc! { "$gte": startDate });
    } else if let Some(endDate) = endDate {
        has_endDate = true;
        filter.insert("JULD", mongodb::bson::doc! { "$lt": endDate });
    }

    let mut has_data = false;
    if !data.is_empty() {
        has_data = true;
        filter.insert("STATION_PARAMETERS", mongodb::bson::doc! { "$all": data.clone() });
    }

    let mut has_id = false;
    if let Some(id) = id {
        has_id = true;
        filter.insert("_id", id);
    }

    // Search for documents with matching filters
    let options_builder = {
        let hint_doc = if has_polygon && !has_startDate && !has_endDate && !has_data && !has_id {
            mongodb::bson::doc! { "geolocation": "2dsphere" }
        } else {
            mongodb::bson::doc! {}
        };
    
        FindOptions::builder()
            .sort(mongodb::bson::doc! { "JULD": -1 })
            .skip(page * (page_size as u64))
            .limit(page_size)
            .hint(mongodb::options::Hint::Keys(hint_doc))
    };

    let mut cursor = {
        let options = options_builder.build();
        let guard = match CLIENT.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let client = guard.as_ref().unwrap();
        client.database("argo").collection::<MapSchema>("argo_search").find(filter, options).await.unwrap()
    }; // in theory the mutex is unlocked here, holding it as little as possible   

    let mut results = Vec::new();

    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                results.push(document);
            },
            Err(e) => {
                eprintln!("Error: {}", e);
                return HttpResponse::InternalServerError().finish();
            }
        }
    }

    if mode == "map" {
        return HttpResponse::Ok().json(results);
    } else {
        let ids: Vec<_> = results.iter().map(|document| document._id.clone()).collect();

        let filter = mongodb::bson::doc! { "_id": { "$in": ids } };
        
        let mut projection = mongodb::bson::doc! { 
            "_id": 1,
            "geolocation": 1,
            "CYCLE_NUMBER": 1,
            "DIRECTION": 1,
            "DATA_STATE_INDICATOR": 1,
            "DATA_MODE": 1,
            "DATE_CREATION": 1,
            "DATE_UPDATE": 1,
            "DC_REFERENCE": 1,
            "JULD": 1,
            "JULD_QC": 1,
            "JULD_LOCATION": 1,
            "POSITION_QC": 1,
            "VERTICAL_SAMPLING_SCHEME": 1,
            "CONFIG_MISSION_NUMBER": 1,
            "STATION_PARAMETERS": 1,
            "data_info": 1,
            "source_file": 1,                 
        };
        if has_data{
            for item in &data {
                projection.insert(&format!("realtime_data.{}", item), 1);
                projection.insert(&format!("adjusted_data.{}", item), 1);
                projection.insert(&format!("level_qc.{}", item), 1);
                projection.insert(&format!("adjusted_level_qc.{}", item), 1);
            }
        } else {
            projection.insert("realtime_data", 1);
            projection.insert("adjusted_data", 1);
            projection.insert("level_qc", 1);
            projection.insert("adjusted_level_qc", 1);
        }

        let data_options_builder = {        
            FindOptions::builder()
                .projection(projection)
        };

        let mut cursor = {
            let options = data_options_builder.build();
            let guard = match CLIENT.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let client = guard.as_ref().unwrap();
            client.database("argo").collection::<DataSchema>("argo").find(filter, options).await.unwrap()
        }; 
        
        let mut matching_results = Vec::new();
        
        while let Some(result) = cursor.next().await {
            match result {
                Ok(mut document) => {
                    // qc filtering
                    for (key, qc_values) in &data_map {
                        if !qc_values.is_empty() {
                            if let Some(realtime_data) = &mut document.realtime_data {
                                if let Some(level_qc) = document.level_qc.as_ref() {
                                    if let Some(level_qc_values) = level_qc.get(key) {
                                        apply_qc_filter(realtime_data, &level_qc_values.clone(), qc_values);
                                    }
                                }
                            }
                            if let Some(adjusted_data) = &mut document.adjusted_data {
                                if let Some(adjusted_level_qc) = document.adjusted_level_qc.as_ref() {
                                    if let Some(adjusted_level_qc_values) = adjusted_level_qc.get(key) {
                                        apply_qc_filter(adjusted_data, &adjusted_level_qc_values.clone(), qc_values);
                                    }
                                }
                            }
                            if let Some(level_qc) = &mut document.level_qc {
                                if let Some(level_qc_values) = level_qc.get(key) {
                                    apply_qc_filter(level_qc, &level_qc_values.clone(), qc_values);
                                }
                            }
                            if let Some(adjusted_level_qc) = &mut document.adjusted_level_qc {
                                if let Some(adjusted_level_qc_values) = adjusted_level_qc.get(key) {
                                    apply_qc_filter(adjusted_level_qc, &adjusted_level_qc_values.clone(), qc_values);
                                }
                            }
                        }
                    }
    
                    // pressure filtering
                    // note you should probably do a pressure qc filter if you're going to do a pressure range filter
                    if !pres_range.is_empty() {
                        if let Some(realtime_data) = &mut document.realtime_data {
                            if let Some(pressures) = realtime_data.get("PRES") {
                                let pressures = pressures.clone();
                                apply_pressure_range(realtime_data, &pressures, &pres_range);
                                if let Some(level_qc) = &mut document.level_qc {
                                    apply_pressure_range(level_qc, &pressures, &pres_range);
                                }
                            }
                        }
                        if let Some(adjusted_data) = &mut document.adjusted_data {
                            if let Some(pressures) = adjusted_data.get("PRES") {
                                let pressures = pressures.clone();
                                apply_pressure_range(adjusted_data, &pressures, &pres_range);
                                if let Some(adjusted_level_qc) = &mut document.adjusted_level_qc {
                                    apply_pressure_range(adjusted_level_qc, &pressures, &pres_range);
                                }
                            }
                        }
                    }
    
                    // only push the document if it still has data for every requested data value after depth and qc filtering
                    let mut should_push = true;
                    for key in data_map.keys() {
                        let realtime_data_empty = document.realtime_data.as_ref()
                            .map_or(true, |data| data.get(key).map_or(true, Vec::is_empty));
                        let adjusted_data_empty = document.adjusted_data.as_ref()
                            .map_or(true, |data| data.get(key).map_or(true, Vec::is_empty));
                    
                        if realtime_data_empty && adjusted_data_empty {
                            should_push = false;
                            break;
                        }
                    }
                    if should_push {
                        matching_results.push(document);
                    }
                },
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return HttpResponse::InternalServerError().finish();
                }
            }
        }
        matching_results.sort_by(|a, b| a.JULD.partial_cmp(&b.JULD).unwrap_or(Ordering::Equal));
        return HttpResponse::Ok().json(matching_results);
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    // Initialize the MongoDB client
    let client_options = mongodb::options::ClientOptions::parse(env::var("MONGODB_URI").unwrap()).await.unwrap();
    let client = mongodb::Client::with_options(client_options).unwrap();

    // Store the client in the static variable
    *CLIENT.lock().unwrap() = Some(client);

    HttpServer::new(|| {
        App::new()
            .service(get_query_params)
            .service(search_data_schema)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}

fn slice_vector_by_pressure_range<T: Clone>(pres_range: &[f64], pressures: &[f64], values: &[T]) -> Vec<T> {
    if pressures.is_empty() || values.is_empty() || pres_range[0] > pressures[pressures.len() - 1] || pres_range[1] < pressures[0]{
        return Vec::new();
    }

    let start_index = pressures.iter().position(|&p| p >= pres_range[0]).unwrap_or(0);
    let end_index = pressures.iter().rposition(|&p| p < pres_range[1]).unwrap_or_else(|| pressures.len() - 1);

    if start_index > end_index {
        Vec::new()
    } else {
        values[start_index..=end_index].to_vec()
    }
}

fn apply_pressure_range<T: Clone + 'static>(data: &mut HashMap<String, Vec<T>>, pressures: &[f64], pres_range: &[f64]) {
    for (key, values) in data.iter_mut() {
        *values = slice_vector_by_pressure_range(pres_range, pressures, values);
    }
}

fn qc_filter<T: Clone>(qc_values: &[String], data: &[T], acceptable_qc: &[i32]) -> Vec<T> {
    if data.is_empty() {
        return Vec::new();
    }

    qc_values.iter()
        .enumerate()
        .filter_map(|(i, qc)| qc.parse::<i32>().ok().and_then(|qc| if acceptable_qc.contains(&qc) { Some(data[i].clone()) } else { None }))
        .collect()
}

fn apply_qc_filter<T: Clone>(data: &mut HashMap<String, Vec<T>>, qc_data: &[String], acceptable_qc: &[i32]) {
    for values in data.values_mut() {
        *values = qc_filter(qc_data, values, acceptable_qc);
    }
}