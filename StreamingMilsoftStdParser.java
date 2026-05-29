package com.cim.streaming;

import com.cim.model.cim.CIMObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Milsoft WindMil ".STD" (Standard ASCII) parser — streaming, O(1) memory.
 *
 * This is the REAL WindMil export format (distinct from the INI-style
 * sectioned CSV handled by {@link StreamingMilsoftCsvParser}).
 *
 * FILE SHAPE (see file_layouts.pdf, "Section File Layout (.STD)"):
 *
 *   Line 1 (optional banner):
 *     MILSOFT STD WM ASCII,001    :&lt;ModelName&gt; Exported by WindMil on ...
 *
 *   Each subsequent line = ONE section/device.  Comma-delimited, NOT fixed
 *   width.  Empty field = two consecutive commas (use default).  First four
 *   fields are mandatory on every row:
 *
 *     col 0  A-1  Section Identifier   (unique name, e.g. "span_10289")
 *     col 1  A-2  Section Type         (integer code, e.g. 1, 9, 10)
 *     col 2  A-3  Phase Configuration  (1..7 bitmask; 7 = ABC)
 *     col 3  A-4  Prior/Parent Id      ("ROOT" or "0" = no parent)
 *     col 4  A-5  Map Number
 *     col 5  A-6  X Coordinate
 *     col 6  A-7  Y Coordinate
 *     col 7  A-8  Preferred Section Name
 *     col 8+      type-dependent fields (interpreted per Section Type)
 *
 * SECTION TYPE → CIM class (grounded in file_layouts.pdf + PNNL-34946):
 *     1  Overhead Line      → ACLineSegment
 *     2  Capacitor          → LinearShuntCompensator
 *     3  Underground Line   → ACLineSegment
 *     4  Regulator          → RatioTapChanger
 *     5  Step Transformer   → PowerTransformer
 *     6  Electric Switch    → Switch
 *     8  Node/Fake          → ConnectivityNode
 *     9  Source             → EnergySource
 *     10 Overcurrent Device → ProtectedSwitch  (recloser/breaker/sectionaliser/fuse)
 *     11 Motor              → AsynchronousMachine
 *     12 Generator          → SynchronousMachine
 *     0  End of Section     → skipped (mapping-system marker, not a device)
 *     7  (undefined)        → skipped
 *
 * IDENTITY / TOPOLOGY:
 *   rdfId = col 0 (the raw Milsoft identifier, verbatim).  The parent link
 *   (col 3) is emitted as a reference whose value is the raw parent id, so
 *   StreamingReferenceResolver matches it against the parent row's rdfId
 *   (foreignField:'rdfId') within the same jobId.  "ROOT"/"0" parents emit
 *   no reference.
 *
 * ATTRIBUTE KEYS:
 *   Universal fields → CIM paths under the default (cim) namespace, e.g.
 *     IdentifiedObject.name, PositionPoint.xPosition, ACDCTerminal.phases.
 *   Everything raw/positional is also preserved as Milsoft.col_&lt;n&gt; so
 *   raw_objects keeps the full file losslessly.  Type-specific named fields
 *   are added where the spec makes them unambiguous (see decorate*()).
 *
 *   Note: vendor attributes use the "Milsoft." (capital M, dot) form so they
 *   share the default cim-namespace prefix extraction (no ':' → treated as
 *   cim).  If you want them under a dedicated "milsoft:" namespace, change
 *   PFX below to "milsoft:" and seed that namespace as enabled.
 */
@Service
public class StreamingMilsoftStdParser {

    private static final Logger log =
            LoggerFactory.getLogger(StreamingMilsoftStdParser.class);

    /** Vendor attribute prefix.  See class note. */
    private static final String PFX = "Milsoft.";

    private static final Map<Integer, String> TYPE_TO_CIM = new HashMap<>();
    static {
        TYPE_TO_CIM.put(1,  "ACLineSegment");
        TYPE_TO_CIM.put(2,  "LinearShuntCompensator");
        TYPE_TO_CIM.put(3,  "ACLineSegment");
        TYPE_TO_CIM.put(4,  "RatioTapChanger");
        TYPE_TO_CIM.put(5,  "PowerTransformer");
        TYPE_TO_CIM.put(6,  "Switch");
        TYPE_TO_CIM.put(8,  "ConnectivityNode");
        TYPE_TO_CIM.put(9,  "EnergySource");
        TYPE_TO_CIM.put(10, "ProtectedSwitch");
        TYPE_TO_CIM.put(11, "AsynchronousMachine");
        TYPE_TO_CIM.put(12, "SynchronousMachine");
        // 0 and 7 intentionally absent → rows skipped.
    }

    /** Phase config code (col 2) → CIM ABC phase string. */
    private static final Map<Integer, String> PHASE_MAP = new HashMap<>();
    static {
        PHASE_MAP.put(1, "A");
        PHASE_MAP.put(2, "B");
        PHASE_MAP.put(3, "C");
        PHASE_MAP.put(4, "AB");
        PHASE_MAP.put(5, "AC");
        PHASE_MAP.put(6, "BC");
        PHASE_MAP.put(7, "ABC");
    }

    public void stream(InputStream in, String fileName,
                       Consumer<CIMObject> onObject) throws Exception {
        long total = 0, skipped = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16)) {

            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.isEmpty()) continue;

                // Skip the banner row (first data field is the literal
                // "MILSOFT STD WM ASCII" token).
                if (lineNum == 1 && line.toUpperCase().contains("MILSOFT STD WM ASCII")) {
                    continue;
                }

                try {
                    CIMObject obj = parseRow(line, fileName, lineNum);
                    if (obj != null) {
                        onObject.accept(obj);
                        total++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    skipped++;
                    log.warn("STD parse error line {}: {}", lineNum, e.getMessage());
                }
            }
        }
        log.info("Milsoft .STD parse complete: {} objects ({} skipped) from {}",
                total, skipped, fileName);
    }

    private CIMObject parseRow(String line, String fileName, int lineNum) {
        String[] f = splitCsv(line);
        if (f.length < 4) return null;   // need the 4 mandatory fields

        String id        = trim(f[0]);
        String typeRaw   = trim(f[1]);
        String phaseRaw  = trim(f[2]);
        String parentRaw = trim(f[3]);

        if (id.isEmpty()) return null;

        Integer typeCode = parseIntOrNull(typeRaw);
        if (typeCode == null) return null;            // non-numeric type → skip
        if (typeCode == 0 || typeCode == 7) return null;  // markers / undefined

        String cimType = TYPE_TO_CIM.get(typeCode);
        if (cimType == null) {
            // Unknown but numeric type — keep it, namespaced, rather than drop.
            cimType = "MilsoftType" + typeCode;
        }

        // rdfId = raw Milsoft identifier (verbatim) so parent refs resolve.
        CIMObject o = new CIMObject(cimType, id);
        o.setSourceFile(fileName);
        o.setSourceFormat("MILSOFT_CSV");

        // Name: prefer the Preferred Section Name (col 7) when present,
        // otherwise fall back to the identifier itself.
        String preferred = f.length > 7 ? trim(f[7]) : "";
        o.setAttribute("IdentifiedObject.name",
                preferred.isEmpty() ? id : preferred);

        // Phases.
        Integer phaseCode = parseIntOrNull(phaseRaw);
        if (phaseCode != null && PHASE_MAP.containsKey(phaseCode)) {
            o.setAttribute("ACDCTerminal.phases", PHASE_MAP.get(phaseCode));
        }
        if (!phaseRaw.isEmpty()) o.setAttribute(PFX + "phaseCode", phaseRaw);

        // Parent topology link → reference (resolves against parent rdfId).
        if (!parentRaw.isEmpty()
                && !"ROOT".equalsIgnoreCase(parentRaw)
                && !"0".equals(parentRaw)) {
            o.addReference(PFX + "parent", parentRaw);
            o.setAttribute(PFX + "parentId", parentRaw);
        }

        // Map number (col 4).
        if (f.length > 4 && !trim(f[4]).isEmpty()) {
            o.setAttribute(PFX + "mapNumber", trim(f[4]));
        }

        // Coordinates (col 5 X, col 6 Y).
        if (f.length > 5 && !trim(f[5]).isEmpty()) {
            o.setAttribute("PositionPoint.xPosition", trim(f[5]));
        }
        if (f.length > 6 && !trim(f[6]).isEmpty()) {
            o.setAttribute("PositionPoint.yPosition", trim(f[6]));
        }

        // Type code itself, for traceability/filtering.
        o.setAttribute(PFX + "sectionType", typeRaw);

        // Type-specific named fields where the spec is unambiguous.
        decorateByType(o, typeCode, f);

        // Preserve EVERY non-empty column verbatim for the raw_objects audit
        // trail.  Keyed by position so the original layout is recoverable.
        for (int i = 0; i < f.length; i++) {
            String v = trim(f[i]);
            if (!v.isEmpty()) o.setAttribute(PFX + "col_" + i, v);
        }

        return o;
    }

    /**
     * Add a few well-understood named fields per Section Type.  Conservative:
     * only fields whose meaning is unambiguous from the spec are named; the
     * rest remain available as Milsoft.col_&lt;n&gt;.
     */
    private void decorateByType(CIMObject o, int type, String[] f) {
        switch (type) {
            case 1:  // Overhead Line
            case 3:  // Underground Line
                // col 8/9/10 = per-phase conductor descriptions (equipment-DB labels)
                putIf(o, f, 8,  PFX + "conductorA");
                putIf(o, f, 9,  PFX + "conductorB");
                putIf(o, f, 10, PFX + "conductorC");
                putIf(o, f, 11, PFX + "conductorNeutral");
                // col 12 = impedance length in FEET
                putIf(o, f, 12, "Conductor.length");
                putIf(o, f, 13, PFX + "constructionType");
                o.setAttribute(PFX + "lineType", type == 1 ? "overhead" : "underground");
                break;
            case 9:  // Source
                // S-12 bus voltage (pu), S-15 nominal voltage (kV)
                putIf(o, f, 11, PFX + "busVoltagePU");
                putIf(o, f, 14, "EnergySource.nominalVoltage");
                putIf(o, f, 16, PFX + "wyeDelta");
                putIf(o, f, 17, PFX + "regulationCode");
                break;
            case 10: // Overcurrent Device (recloser/breaker/sectionaliser/fuse)
                putIf(o, f, 8,  PFX + "deviceA");
                putIf(o, f, 9,  PFX + "deviceB");
                putIf(o, f, 10, PFX + "deviceC");
                // O-12/13/14 = is-closed per phase (1 = closed). normalOpen is
                // the inverse of "closed" for the first existing phase.
                String closedA = f.length > 11 ? trim(f[11]) : "";
                if (!closedA.isEmpty()) {
                    boolean closed = "1".equals(closedA) || "true".equalsIgnoreCase(closedA);
                    o.setAttribute("Switch.normalOpen", closed ? "false" : "true");
                    o.setAttribute(PFX + "isClosedA", closedA);
                }
                break;
            case 6:  // Electric Switch — E-9 status O/C/L
                String status = f.length > 8 ? trim(f[8]) : "";
                if (!status.isEmpty()) {
                    // Spec: O=Open, C=Closed, L=Looped. Map to normalOpen.
                    boolean open = status.equalsIgnoreCase("O");
                    o.setAttribute("Switch.normalOpen", open ? "true" : "false");
                    o.setAttribute(PFX + "switchStatus", status);
                }
                break;
            case 5:  // Step Transformer
                putIf(o, f, 10, PFX + "ratedInputVoltage");
                putIf(o, f, 13, PFX + "ratedOutputVoltage");
                break;
            case 4:  // Regulator
                putIf(o, f, 8,  PFX + "regulatorType");
                putIf(o, f, 9,  PFX + "controllingPhase");
                break;
            default:
                // No named decoration; col_<n> already captures everything.
                break;
        }
    }

    private void putIf(CIMObject o, String[] f, int idx, String key) {
        if (f.length > idx) {
            String v = trim(f[idx]);
            if (!v.isEmpty()) o.setAttribute(key, v);
        }
    }

    /** Returns true if this looks like a WindMil .STD file (banner present). */
    public static boolean looksLikeStd(String firstLine) {
        return firstLine != null
                && firstLine.toUpperCase().contains("MILSOFT STD WM ASCII");
    }

    // ── CSV splitter (handles basic double-quote quoting) ─────────────────
    private String[] splitCsv(String line) {
        java.util.List<String> out = new java.util.ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQ = !inQ;
            } else if (c == ',' && !inQ) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out.toArray(new String[0]);
    }

    private static String trim(String s) { return s == null ? "" : s.trim(); }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Integer.valueOf(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }
}
