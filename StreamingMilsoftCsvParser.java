package com.cim.streaming;

import com.cim.model.cim.CIMObject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

/**
 * Milsoft CSV File Parser — Streaming, O(1) memory.
 *
 * MILSOFT FILE FORMAT:
 *   Milsoft WindMil exports use a multi-section CSV format.
 *   Each section starts with a header line declaring the equipment type:
 *
 *   [Line]                          ← section header = CIM type
 *   ID,Name,FromNode,ToNode,R1,X1,B1,Length
 *   1001,BONN_NBON_4230A,N100,N200,0.111,1.031,0.0000,45000
 *   1002,WEST_EAST_1380,N300,N400,0.092,0.881,0.0000,32000
 *
 *   [Switch]
 *   ID,Name,Node1,Node2,NormalOpen,Type
 *   2001,BKR-WEST,N100,N150,0,Breaker
 *   2002,DSC-EAST,N200,N250,1,Disconnector
 *
 *   [Transformer]
 *   ID,Name,Node1,Node2,kVA,PrimaryKV,SecondaryKV
 *   3001,XFMR-MAIN,N100,N110,10000,230,115
 *
 * MILSOFT → CIM MAPPING:
 *   [Line]        → ACLineSegment
 *   [Switch]      → Breaker / Disconnector / Fuse (from Type column)
 *   [Transformer] → PowerTransformer
 *   [Bus]         → ConnectivityNode
 *   [Load]        → EnergyConsumer
 *   [Generator]   → SynchronousMachine
 *   [Capacitor]   → LinearShuntCompensator
 *   [Regulator]   → TapChanger
 *   [Substation]  → Substation
 *
 * ATTRIBUTE MAPPING:
 *   Column names → stored as {CimType}.{attributeName}
 *   e.g. Line.R1 → ACLineSegment.r
 *        Switch.NormalOpen → Switch.normalOpen
 *
 * rdfId: generated as "_milsoft_{sectionType}_{ID}"
 */
@Service
public class StreamingMilsoftCsvParser {

    private static final Logger log =
            LoggerFactory.getLogger(StreamingMilsoftCsvParser.class);

    // ── Milsoft section name → CIM type mapping ───────────────────────────
    //
    // Class assignments grounded in IEC 61970 + PNNL-34946 CIM17v40 guide.
    // Corrections from the original mapping (see commit history):
    //   SOURCEBUS  was TopologicalNode  → EnergySource     (logical → physical)
    //   BUS        was ConnectivityNode → BusbarSection    (logical → physical)
    //   REGULATOR  was TapChanger       → RatioTapChanger  (abstract → concrete)
    //   FEEDER     was Line             → Feeder           (transmission → distribution)
    //
    // Note on REGULATOR: in strict CIM, a line voltage regulator is modelled
    // as a PowerTransformer with a RatioTapChanger attached to one end.  We
    // emit only the RatioTapChanger here (one CIMObject per row).  If your
    // downstream consumers need the paired PowerTransformer, that needs a
    // separate enrichment pass — flagged for follow-up.
    private static final Map<String, String> SECTION_TO_CIM = new LinkedHashMap<>();
    static {
        SECTION_TO_CIM.put("LINE",         "ACLineSegment");
        SECTION_TO_CIM.put("SWITCH",       "Switch");       // refined per-row by Type col
        SECTION_TO_CIM.put("TRANSFORMER",  "PowerTransformer");
        SECTION_TO_CIM.put("BUS",          "BusbarSection");        // physical busbar
        SECTION_TO_CIM.put("LOAD",         "EnergyConsumer");
        SECTION_TO_CIM.put("GENERATOR",    "SynchronousMachine");
        SECTION_TO_CIM.put("CAPACITOR",    "LinearShuntCompensator");
        SECTION_TO_CIM.put("REGULATOR",    "RatioTapChanger");      // voltage regulator
        SECTION_TO_CIM.put("SUBSTATION",   "Substation");
        SECTION_TO_CIM.put("FEEDER",       "Feeder");               // distribution feeder
        SECTION_TO_CIM.put("NODE",         "ConnectivityNode");     // logical join point
        SECTION_TO_CIM.put("SOURCEBUS",    "EnergySource");         // substation source
        SECTION_TO_CIM.put("SOURCE",       "EnergySource");         // alias
        SECTION_TO_CIM.put("AREA",         "SubGeographicalRegion");
        SECTION_TO_CIM.put("ZONE",         "GeographicalRegion");
    }

    // ── Milsoft column → CIM attribute mapping per section ───────────────
    // Format: column name (uppercase) → CIM attribute name
    private static final Map<String, Map<String, String>> COL_MAP = new HashMap<>();
    static {
        // LINE section
        Map<String, String> line = new LinkedHashMap<>();
        line.put("R1",       "ACLineSegment.r");
        line.put("X1",       "ACLineSegment.x");
        line.put("B1",       "ACLineSegment.bch");
        line.put("R0",       "ACLineSegment.r0");
        line.put("X0",       "ACLineSegment.x0");
        line.put("LENGTH",   "Conductor.length");
        line.put("RATING",   "CurrentLimit.value");
        line.put("FROMNODE", "ref:ACLineSegment.fromNode");  // ref: prefix = reference
        line.put("TONODE",   "ref:ACLineSegment.toNode");
        COL_MAP.put("LINE", line);

        // SWITCH section
        Map<String, String> sw = new LinkedHashMap<>();
        sw.put("NORMALOPEN", "Switch.normalOpen");
        sw.put("TYPE",       "milsoft:switchType");          // drives cimType refinement
        sw.put("NODE1",      "ref:Switch.node1");
        sw.put("NODE2",      "ref:Switch.node2");
        sw.put("RATING",     "CurrentLimit.value");
        COL_MAP.put("SWITCH", sw);

        // TRANSFORMER section
        Map<String, String> xfmr = new LinkedHashMap<>();
        xfmr.put("KVA",         "PowerTransformer.ratedS");
        xfmr.put("PRIMARYKV",   "TransformerEnd.ratedU");
        xfmr.put("SECONDARYKV", "milsoft:secondaryKV");
        xfmr.put("IMPEDANCEP",  "TransformerMeshImpedance.r");
        xfmr.put("NODE1",       "ref:PowerTransformer.node1");
        xfmr.put("NODE2",       "ref:PowerTransformer.node2");
        xfmr.put("RATING",      "CurrentLimit.value");
        COL_MAP.put("TRANSFORMER", xfmr);

        // BUS/NODE section
        Map<String, String> bus = new LinkedHashMap<>();
        bus.put("BASEKV",    "BaseVoltage.nominalVoltage");
        bus.put("VOLTAGE",   "SvVoltage.v");
        bus.put("ANGLE",     "SvVoltage.angle");
        bus.put("TYPE",      "milsoft:busType");
        COL_MAP.put("BUS", bus);
        COL_MAP.put("NODE", bus);

        // LOAD section
        Map<String, String> load = new LinkedHashMap<>();
        load.put("KW",      "EnergyConsumer.p");
        load.put("KVAR",    "EnergyConsumer.q");
        load.put("NODE",    "ref:EnergyConsumer.node");
        load.put("PHASES",  "ConductingEquipment.phases");
        COL_MAP.put("LOAD", load);

        // GENERATOR section
        Map<String, String> gen = new LinkedHashMap<>();
        gen.put("KW",       "SynchronousMachine.p");
        gen.put("KVAR",     "SynchronousMachine.q");
        gen.put("KVA",      "SynchronousMachine.ratedS");
        gen.put("NODE",     "ref:SynchronousMachine.node");
        gen.put("TYPE",     "milsoft:generatorType");
        COL_MAP.put("GENERATOR", gen);

        // CAPACITOR section
        Map<String, String> cap = new LinkedHashMap<>();
        cap.put("KVAR",     "LinearShuntCompensator.bPerSection");
        cap.put("NODE",     "ref:LinearShuntCompensator.node");
        cap.put("PHASES",   "ConductingEquipment.phases");
        COL_MAP.put("CAPACITOR", cap);

        // SUBSTATION section
        Map<String, String> sub = new LinkedHashMap<>();
        sub.put("REGION",   "ref:Substation.region");
        sub.put("AREA",     "ref:Substation.area");
        COL_MAP.put("SUBSTATION", sub);

        // ── Sections below were added to extend coverage per spec ──────────
        //
        // Column names are inferred from the WindMil .STD field meanings
        // (file_layouts.pdf, Section File Layout) BUT your CSV dialect uses
        // named headers rather than positional columns.  Verify each column
        // name below against an actual sample row before relying on these in
        // production — unmapped columns still fall through to the
        // `milsoft:<colname>` vendor-namespaced attribute, so there is no
        // data loss if a column name doesn't match.

        // SOURCE section (corresponds to .STD Section Type 9, substation source)
        // Source rows carry per-unit bus voltage (S-12), nominal voltage (S-15),
        // wye/delta code (S-17), regulation code (S-18) per the spec.
        Map<String, String> src = new LinkedHashMap<>();
        src.put("BUSVOLTAGE",       "EnergySource.voltageMagnitude");  // per-unit (S-12)
        src.put("NOMINALVOLTAGE",   "EnergySource.nominalVoltage");     // kV L-G or L-L (S-15)
        src.put("CONNECTION",       "milsoft:wyeDelta");                // W or D (S-17)
        src.put("REGULATION",       "milsoft:regulationCode");          // R or U (S-18)
        src.put("SUBSTATION",       "ref:EnergySource.substation");
        src.put("NODE",             "ref:EnergySource.node");
        // Min/max source impedance equipment-DB labels (S-9, S-10).
        // Kept as vendor attributes — they reference the equipment catalog,
        // not raw R/X values.
        src.put("ZSMMIN",           "milsoft:zsmImpedanceMin");
        src.put("ZSMMAX",           "milsoft:zsmImpedanceMax");
        COL_MAP.put("SOURCE",    src);
        COL_MAP.put("SOURCEBUS", src);    // same column layout under either name

        // REGULATOR section (corresponds to .STD Section Type 4)
        // Voltage regulator — emitted as RatioTapChanger.  Spec fields R-9..R-29
        // cover regulator type (3φ-dep vs 1φ-indep), controlling phase, winding
        // connection, output voltage targets per phase, LDC R/X settings, and
        // first-house high/low protectors.
        Map<String, String> reg = new LinkedHashMap<>();
        reg.put("REGULATORTYPE",     "milsoft:regulatorType");        // 0=3φ-dep, 1=1φ-indep
        reg.put("CONTROLLINGPHASE",  "milsoft:controllingPhase");
        reg.put("WINDINGCONNECTION", "milsoft:windingConnection");
        // Output voltage target — per-unit; LDC target if LDC enabled.
        reg.put("OUTPUTVOLTAGE",     "RatioTapChanger.stepVoltageIncrement");
        reg.put("LDCR",              "milsoft:ldcR");                 // line-drop compensation R
        reg.put("LDCX",              "milsoft:ldcX");                 // line-drop compensation X
        reg.put("HIGHPROTECTOR",     "milsoft:firstHouseHigh");
        reg.put("LOWPROTECTOR",      "milsoft:firstHouseLow");
        reg.put("FROMNODE",          "ref:RatioTapChanger.fromNode");
        reg.put("TONODE",            "ref:RatioTapChanger.toNode");
        COL_MAP.put("REGULATOR", reg);

        // Capacitor extra fields per .STD spec (C-12 through C-19).
        // The base CAPACITOR map (above) already has KVAR + NODE + PHASES;
        // this overlay augments it for parsers that surface the switch fields.
        cap.put("VOLTAGERATING",  "milsoft:voltageRating");          // C-12
        cap.put("SWITCHTYPE",     "milsoft:capSwitchType");          // C-13 (0=manual..5=temp)
        cap.put("SWITCHSTATUS",   "milsoft:capSwitchStatus");        // C-14 (0=disc,1=on,2=off)
        cap.put("SWITCHONSETTING",  "milsoft:capSwitchOn");          // C-15
        cap.put("SWITCHOFFSETTING", "milsoft:capSwitchOff");         // C-16
        cap.put("CONNECTION",     "milsoft:capConnection");          // C-18

        // Transformer extra fields per .STD spec (T-9 through T-19).
        xfmr.put("WINDINGCONNECTION",  "milsoft:windingConnection"); // T-9 (code 1..14)
        xfmr.put("APCNF",              "milsoft:apcnf");             // T-15 (open Y-D config)
        xfmr.put("TERTIARYKV",         "milsoft:tertiaryKV");        // T-16
        xfmr.put("TERTIARYCHILD",      "ref:milsoft:tertiaryChild"); // T-17

        // Line extra fields per .STD spec (L-12 neutral, L-13 length-in-feet,
        // L-14 construction, L-17 load location, L-18 load growth).
        // L-13 (Impedance Length) is already covered by your LENGTH column.
        line.put("CONDUCTORNEUTRAL",   "milsoft:conductorNeutral");  // L-12
        line.put("CONSTRUCTION",       "milsoft:constructionType");  // L-14
        line.put("LOADLOCATION",       "milsoft:loadLocation");      // L-17 (U/L/S)
        line.put("LOADGROWTH",         "milsoft:loadGrowth");        // L-18
    }

    // ── Switch type refinement ───────────────────────────────────────────
    private static final Map<String, String> SWITCH_TYPE_MAP = new HashMap<>();
    static {
        SWITCH_TYPE_MAP.put("BREAKER",      "Breaker");
        SWITCH_TYPE_MAP.put("DISCONNECTOR", "Disconnector");
        SWITCH_TYPE_MAP.put("FUSE",         "Fuse");
        SWITCH_TYPE_MAP.put("RECLOSER",     "Recloser");
        SWITCH_TYPE_MAP.put("SECTIONALIZER","LoadBreakSwitch");
        SWITCH_TYPE_MAP.put("JUMPER",       "Jumper");
    }

    // ── Main parse method ─────────────────────────────────────────────────

    /**
     * Stream-parse a Milsoft CSV file.
     * Calls onObject once per CIM object found.
     * Memory: O(1) — processes one row at a time.
     *
     * @param in        input stream of the .csv file
     * @param fileName  original filename (stored on each object)
     * @param onObject  callback — called for each parsed CIMObject
     */
    public void stream(InputStream in, String fileName,
                        Consumer<CIMObject> onObject) throws Exception {
        long total = 0;
        String currentSection = null;
        List<String> headers   = null;
        Map<String, String> colMapping = null;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {

            String line;
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();

                // Skip empty lines and comment lines
                if (line.isEmpty() || line.startsWith("//")
                        || line.startsWith("#") || line.startsWith(";")) {
                    continue;
                }

                // Section header: [SectionName]
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1)
                            .trim().toUpperCase();
                    headers   = null;
                    colMapping = COL_MAP.getOrDefault(currentSection, new HashMap<>());
                    log.debug("CSV section: [{}]", currentSection);
                    continue;
                }

                if (currentSection == null) continue;

                // First non-blank line after section header = column headers
                if (headers == null) {
                    headers = parseHeaderLine(line);
                    continue;
                }

                // Data row — parse into CIMObject
                try {
                    CIMObject obj = parseRow(line, headers, colMapping,
                            currentSection, fileName, lineNum);
                    if (obj != null) {
                        onObject.accept(obj);
                        total++;
                    }
                } catch (Exception e) {
                    log.warn("CSV parse error line {}: {} — {}", lineNum, line, e.getMessage());
                }
            }
        }

        log.info("Milsoft CSV parse complete: {} objects from {}", total, fileName);
    }

    // ── Parse single data row ─────────────────────────────────────────────

    private CIMObject parseRow(String line, List<String> headers,
                                Map<String, String> colMapping,
                                String section, String fileName,
                                int lineNum) {
        // Simple CSV split (handles basic quoting)
        List<String> values = splitCsvLine(line);
        if (values.isEmpty()) return null;

        // Need at least ID and Name columns
        int idIdx   = indexOfIgnoreCase(headers, "ID");
        int nameIdx = indexOfIgnoreCase(headers, "NAME");

        String rowId   = idIdx   >= 0 && idIdx   < values.size() ? values.get(idIdx).trim()   : String.valueOf(lineNum);
        String rowName = nameIdx >= 0 && nameIdx < values.size() ? values.get(nameIdx).trim() : "";

        // Determine CIM type.  cimType is fixed at construction time on
        // CIMObject (no setter), so for SWITCH sections we must resolve the
        // refined type (Breaker/Disconnector/Fuse/…) from the Type column
        // BEFORE constructing the object — see resolveCimType().
        String cimType = resolveCimType(section, headers, values, colMapping);

        String rdfId = "_milsoft_" + section.toLowerCase() + "_" + rowId;

        // Construct via the real API: (cimType, rdfId).  This also seeds mrid
        // = rdfId inside the constructor.
        CIMObject obj = new CIMObject(cimType, rdfId);
        obj.setSourceFile(fileName);
        obj.setSourceFormat("MILSOFT_CSV");

        // Name flows through setAttribute("IdentifiedObject.name", …) — the
        // CIMObject derives getName()/mrid from known attribute keys.  Set it
        // explicitly only when we actually have a name value.
        if (!rowName.isEmpty()) {
            obj.setAttribute("IdentifiedObject.name", rowName);
        }

        // Process each column
        for (int i = 0; i < headers.size() && i < values.size(); i++) {
            String colUpper = headers.get(i).toUpperCase();
            String value    = values.get(i).trim();

            if (value.isEmpty()) continue;

            // ID and NAME are already consumed above — don't also dump them
            // into the milsoft: bucket.
            if ("ID".equals(colUpper) || "NAME".equals(colUpper)) continue;

            // Look up CIM mapping for this column
            String cimAttr = colMapping.get(colUpper);

            if (cimAttr == null) {
                // No explicit mapping — store as milsoft: vendor attribute
                obj.setAttribute("milsoft:" + colUpper.toLowerCase(), value);
                continue;
            }

            if (cimAttr.startsWith("ref:")) {
                // Reference attribute — store via addReference
                String refKey = cimAttr.substring(4);
                // Milsoft IDs → generate rdfId for the referenced node
                obj.addReference(refKey, "_milsoft_node_" + value);

            } else if (cimAttr.equals("milsoft:switchType")) {
                // cimType already resolved before construction — just record
                // the raw value as an attribute for traceability.
                obj.setAttribute(cimAttr, value);

            } else if (cimAttr.equals("Switch.normalOpen")) {
                // Normalise: 0/false → "false", 1/true → "true"
                String normalized = ("1".equals(value) || "true".equalsIgnoreCase(value))
                        ? "true" : "false";
                obj.setAttribute(cimAttr, normalized);

            } else {
                obj.setAttribute(cimAttr, value);
            }
        }

        return obj;
    }

    /**
     * Resolve the final CIM type for a row.
     *
     * For most sections this is just the SECTION_TO_CIM lookup.  For SWITCH
     * sections, the concrete type (Breaker / Disconnector / Fuse / Recloser /
     * …) depends on the row's "Type" column, which must be read here because
     * CIMObject fixes cimType at construction.
     */
    private String resolveCimType(String section, List<String> headers,
                                   List<String> values,
                                   Map<String, String> colMapping) {
        String base = SECTION_TO_CIM.getOrDefault(section, "milsoft:" + section);

        // Only SWITCH rows carry a refining Type column.
        if (!"SWITCH".equals(section)) return base;

        // Find which header maps to milsoft:switchType, then read that value.
        for (int i = 0; i < headers.size() && i < values.size(); i++) {
            String colUpper = headers.get(i).toUpperCase();
            String mapped   = colMapping.get(colUpper);
            if ("milsoft:switchType".equals(mapped)) {
                String typeVal = values.get(i).trim();
                String refined = SWITCH_TYPE_MAP.get(typeVal.toUpperCase());
                if (refined != null) return refined;
                break;
            }
        }
        return base;
    }

    // ── Column header parsing ─────────────────────────────────────────────

    private List<String> parseHeaderLine(String line) {
        List<String> headers = new ArrayList<>();
        for (String h : splitCsvLine(line)) {
            headers.add(h.trim().toUpperCase());
        }
        return headers;
    }

    // ── CSV line splitter (handles quoted fields) ─────────────────────────

    private List<String> splitCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder sb    = new StringBuilder();
        boolean inQuotes    = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                result.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(ch);
            }
        }
        result.add(sb.toString());
        return result;
    }

    private int indexOfIgnoreCase(List<String> list, String target) {
        for (int i = 0; i < list.size(); i++) {
            if (target.equalsIgnoreCase(list.get(i))) return i;
        }
        return -1;
    }

    /**
     * Returns true if the given file content looks like a Milsoft CSV.
     * Used by ImportService for auto-detection.
     */
    public boolean canParse(String firstLine) {
        if (firstLine == null) return false;
        String upper = firstLine.trim().toUpperCase();
        return upper.startsWith("[") && SECTION_TO_CIM.containsKey(
                upper.replaceAll("[\\[\\]]", ""));
    }
}
