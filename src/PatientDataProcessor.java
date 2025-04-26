package src;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// You'll need to import Hadoop FileSystem classes if reading directly from HDFS in this class
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.conf.Configuration;

public class PatientDataProcessor {

    public static void main(String[] args) {
        // Check if correct number of arguments are provided
        if (args.length < 2) {
            System.err.println("Usage: PatientDataProcessor <input_csv_path> <output_directory_path>");
            System.exit(1); // Exit if arguments are missing
        }

        String inputCsvPath = args[0]; // First argument is the input CSV path
        String outputDirPath = args[1]; // Second argument is the output directory path

        String line;
        List<Patient> patientList = new ArrayList<>();

        // --- Reading from HDFS (Conceptual - requires Hadoop API) ---
        // When running on YARN, you'll typically use Hadoop's FileSystem API to read from HDFS.
        // The FileReader approach below works for local testing, but needs replacement for HDFS.
        // A simple way for this assignment might be to treat the HDFS path as a regular path IF your YARN setup allows it,
        // but the proper way is using the Hadoop FileSystem API.
        // For simplicity in illustrating the argument passing, we'll keep FileReader for now,
        // but remember this part needs adjustment for true HDFS reading.

        try {
             // For local testing: Use FileReader
             // For HDFS reading on YARN: Replace with Hadoop FileSystem API
             BufferedReader br = new BufferedReader(new FileReader(inputCsvPath)); // This line needs to be adapted for HDFS

            br.readLine(); // Skip header if it exists

            while ((line = br.readLine()) != null) {
                String[] fields = parseCsvLine(line);
                if (fields.length > 0) {
                    Patient patient = createPatientFromFields(fields);
                    patientList.add(patient);
                } else {
                    System.out.println("Skipping invalid line: " + line);
                }
            }
            br.close(); // Close the reader

            // Perform classification and output to JSON files
            classifyAndOutputPatientsToJson(patientList, outputDirPath); // Pass output directory

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String[] parseCsvLine(String line) {
        // ... (parseCsvLine method remains the same) ...
        List<String> values = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentValue = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    currentValue.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                values.add(currentValue.toString().trim());
                currentValue = new StringBuilder();
            } else if (c == '\r' || c == '\n') {
                if (inQuotes) {
                    currentValue.append(c);
                }
            } else {
                currentValue.append(c);
            }
        }

        values.add(currentValue.toString().trim());
        return values.toArray(new String[0]);
    }

    private static Patient createPatientFromFields(String[] fields) {
        // ... (createPatientFromFields method remains the same) ...
        Patient patient = new Patient();

        if (fields.length > 0) {
            patient.setPatientId(fields[0].trim());
        }
        if (fields.length > 1) {
            patient.setName(fields[1].trim());
        }
        if (fields.length > 2) {
            patient.setAge(Integer.parseInt(fields[2].trim()));
        }
        if (fields.length > 3) {
            patient.setGender(fields[3].trim());
        }
        if (fields.length > 12) {
            patient.setRegion(fields[12].trim()); // Adjust index if needed
        }
        if (fields.length > 8) {
            patient.setSymptoms(fields[8].trim()); // Extract symptoms - Adjust index if needed
        }
        return patient;
    }

    private static void classifyAndOutputPatientsToJson(List<Patient> patientList, String outputDirPath) { // Added outputDirPath
        Map<String, Patient> patientIdMap = new HashMap<>();
        Map<String, List<Patient>> regionMap = new HashMap<>();
        Map<String, List<Patient>> symptomMap = new HashMap<>();
        Map<String, String> normalizedToOriginalSymptom = new HashMap<>();

        // Classify patients
        for (Patient patient : patientList) {
            patientIdMap.put(patient.getPatientId(), patient);

            String region = patient.getRegion();
            if (!regionMap.containsKey(region)) {
                regionMap.put(region, new ArrayList<>());
            }
            regionMap.get(region).add(patient);

            String originalSymptom = patient.getSymptoms();
            if (originalSymptom != null && !originalSymptom.isEmpty()) {
                String normalizedSymptom = normalizeSymptom(originalSymptom);
                if (!symptomMap.containsKey(normalizedSymptom)) {
                    symptomMap.put(normalizedSymptom, new ArrayList<>());
                    normalizedToOriginalSymptom.put(normalizedSymptom, originalSymptom);
                }
                symptomMap.get(normalizedSymptom).add(patient);
            }
        }

        // Output to JSON files in the specified output directory
        try {
            // Construct full output paths
            String patientIdOutputPath = outputDirPath + "/patient_id_output.json";
            String regionOutputPath = outputDirPath + "/region_output.json";
            String symptomOutputPath = outputDirPath + "/symptom_output.json";

            writePatientIdOutputToJson(patientIdMap, patientIdOutputPath);
            writeRegionOutputToJson(regionMap, regionOutputPath);
            writeSymptomOutputToJson(symptomMap, normalizedToOriginalSymptom, symptomOutputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method to normalize symptom strings
    private static String normalizeSymptom(String symptom) {
       // ... (normalizeSymptom method remains the same) ...
       String normalized = symptom.toLowerCase();
        normalized = normalized.replaceAll("[^a-z0-9\\s]", "");
        String[] words = normalized.split("\\s+");
        Arrays.sort(words);
        return String.join(" ", words);
    }


    private static void writePatientIdOutputToJson(Map<String, Patient> patientIdMap, String filename) throws IOException {
       // ... (writePatientIdOutputToJson method remains the same, using the filename argument) ...
        JSONArray jsonArray = new JSONArray();
        for (Map.Entry<String, Patient> entry : patientIdMap.entrySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("patientId", entry.getKey());
            jsonObject.put("patient", patientToJson(entry.getValue()));
            jsonArray.put(jsonObject);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(jsonArray.toString(2));
        writer.close();
    }

    private static void writeRegionOutputToJson(Map<String, List<Patient>> regionMap, String filename) throws IOException {
        // ... (writeRegionOutputToJson method remains the same, using the filename argument) ...
        JSONArray jsonArray = new JSONArray();
        for (Map.Entry<String, List<Patient>> entry : regionMap.entrySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("region", entry.getKey());
            JSONArray patientsArray = new JSONArray();
            for (Patient patient : entry.getValue()) {
                patientsArray.put(patientToJson(patient));
            }
            jsonObject.put("patients", patientsArray);
            jsonArray.put(jsonObject);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(jsonArray.toString(2));
        writer.close();
    }

    private static void writeSymptomOutputToJson(Map<String, List<Patient>> symptomMap, Map<String, String> normalizedToOriginalSymptom, String filename) throws IOException {
       // ... (writeSymptomOutputToJson method remains the same, using the filename argument) ...
        JSONArray jsonArray = new JSONArray();
        int groupId = 1;
        for (Map.Entry<String, List<Patient>> entry : symptomMap.entrySet()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("symptomGroupId", groupId++);
            jsonObject.put("symptomLabel", normalizedToOriginalSymptom.get(entry.getKey()));

            JSONArray patientsArray = new JSONArray();
            for (Patient patient : entry.getValue()) {
                patientsArray.put(patientToJson(patient));
            }
            jsonObject.put("patients", patientsArray);
            jsonArray.put(jsonObject);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        writer.write(jsonArray.toString(2));
        writer.close();
    }


    private static JSONObject patientToJson(Patient patient) {
        // ... (patientToJson method remains the same) ...
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("patientId", patient.getPatientId());
        jsonObject.put("name", patient.getName());
        jsonObject.put("age", patient.getAge());
        jsonObject.put("gender", patient.getGender());
        jsonObject.put("region", patient.getRegion());
        jsonObject.put("symptoms", patient.getSymptoms());
        return jsonObject;
    }
}