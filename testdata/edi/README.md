# EDI Test Files

This directory contains sample EDIFACT/EANCOM test files for unit tests, integration tests, and manual validation of the EDI parser and validation engine.

## File Overview

### Valid Files

#### `valid_orders_d96a_minimal.edi`
**Purpose:** Basic parsing validation with minimal required segments.

**Content:**
- UNB/UNZ interchange envelope
- Single UNH/UNT message envelope
- BGM (Begin of Message) with order type 220
- One DTM (date) segment
- One NAD (Name and Address) for buyer party
- One LIN (Line Item) with simple QTY

**Expected Behavior:** Should parse successfully with no validation errors.

**Size:** ~300 bytes

---

#### `valid_orders_d96a_full.edi`
**Purpose:** Comprehensive parsing and mapping tests with all common ORDERS segments.

**Content:**
- Multiple DTM segments (document date, delivery date)
- Multiple RFF segments (order reference, contract reference)
- Three NAD segments (Buyer BY, Supplier SU, Delivery Point DP)
- CUX (Currency) segment with EUR
- Three line items (LIN) with:
  - PIA (Additional Product ID) for supplier and manufacturer codes
  - IMD (Item Description) with product names and barcodes
  - QTY (Quantity) ordered and confirmed
  - PRI (Price) calculation and net price
  - MOA (Monetary Amount) per line
- UNS (Section Control) segment
- CNT (Control) segment with line count
- MOA total segment

**Expected Behavior:** Should parse successfully and exercise all common segment types.

**Size:** ~1.2 KB

---

### Invalid Files (for validation testing)

#### `invalid_orders_missing_bgm.edi`
**Purpose:** Test validation of mandatory segment requirements.

**Content:**
- Valid UNB/UNZ interchange
- Valid UNH/UNT message
- Missing BGM (Begin of Message) segment - starts directly with DTM
- Other segments are valid

**Expected Behavior:** Parser should detect missing mandatory BGM segment and report validation error.

**Error Type:** Missing mandatory segment

---

#### `invalid_orders_unknown_segment.edi`
**Purpose:** Test handling of unrecognized segment tags.

**Content:**
- Valid interchange and message structure
- Contains an invalid segment `XYZ` between NAD and LIN
- XYZ is not a valid EDIFACT segment code

**Expected Behavior:** Parser should either skip unknown segments with warning or fail validation depending on strictness level.

**Error Type:** Unknown segment tag

---

### Edge Case Files

#### `edge_empty_elements.edi`
**Purpose:** Test handling of empty elements (consecutive separators).

**Content:**
- NAD+BY++12345::9 - empty second element (consecutive ++)
- NAD+SU+9876543210987::9+++' - trailing empty elements
- PIA+1+:SA - empty component in composite
- QTY+21:100:EA - standard filled elements
- PRI+AAA:15.99:: - trailing empty components

**Expected Behavior:** Parser should handle empty elements gracefully without errors, preserving the structure.

**Special Characteristics:** Exercises all separator edge cases: element separator (+), component separator (:), and empty trailing elements.

---

#### `edge_special_chars.edi`
**Purpose:** Test release character usage for escaping special characters.

**Content:**
- BGM+220+ORDER?+SPECIAL+9 - escaped + in order number (becomes literal +)
- NAD with company name containing special chars: ACME?&Industrial?+Supplies
  - ?& becomes literal &
  - ?+ becomes literal +
- IMD with ?? and ?+ escape sequences in description

**Expected Behavior:** Parser should interpret release character (?) correctly and unescape the values.

**Special Characteristics:** 
- Tests `?+` → literal `+`
- Tests `??` → literal `?`
- Tests `?&` → literal `&` (segment separator protection)

---

## EDI Format Reference

### Separators
- **Segment Terminator:** `'` (apostrophe)
- **Element Separator:** `+`
- **Component Separator:** `:`
- **Release Character:** `?` (for escaping)

### Common Segment Types (ORDERS D96A)
- **UNB/UNZ:** Interchange envelope (begin/end)
- **UNH/UNT:** Message envelope (begin/end)
- **BGM:** Begin of Message (document type, number, status)
- **DTM:** Date/Time/Period
- **RFF:** Reference (order number, contract, etc.)
- **NAD:** Name and Address (parties: BY=Buyer, SU=Supplier, DP=Delivery)
- **CUX:** Currencies
- **LIN:** Line Item
- **PIA:** Additional Product ID
- **IMD:** Item Description
- **QTY:** Quantity
- **PRI:** Price Details
- **MOA:** Monetary Amount
- **UNS:** Section Control (summary separator)
- **CNT:** Control (line/item counts)

### GLN Codes Used
All GLNs in these files are anonymized fake values:
- 1234567890123 (Buyer)
- 9876543210987 (Supplier)
- 5555555555555 (Delivery point)

### Standards
- **Syntax:** UNOA:3
- **Message Type:** ORDERS
- **Version:** D:96A (EANCOM D96A)
- **Organization:** UN/ECE

## Notes for Testing

1. **Parsing Tests:** All valid files should parse without syntax errors.
2. **Validation Tests:** Invalid files should trigger appropriate validation errors.
3. **Edge Cases:** Empty elements and special characters should be handled correctly.
4. **Mapping Tests:** Full file should exercise all mapping DSL features.
5. **Performance:** These files are small; larger files can be generated for performance testing.

## Adding New Test Files

When adding new test files:
1. Use the naming convention: `{valid|invalid|edge}_{description}_d96a_{modifier}.edi`
2. Document the file in this README
3. Ensure files are syntactically valid (except intentionally invalid ones)
4. Anonymize all real company data (GLNs, names, addresses)
5. Keep file sizes reasonable (< 10KB for normal tests)
