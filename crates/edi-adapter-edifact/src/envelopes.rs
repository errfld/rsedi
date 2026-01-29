//! EDIFACT envelope handling (UNB/UNZ, UNH/UNT, UNA)
//!
//! This module provides parsing, generation, and validation of EDIFACT envelope segments.

use crate::parser::{Element, Segment};
use crate::{Error, Result};
use edi_ir::Position;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Interchange envelope containing all messages
#[derive(Debug, Clone)]
pub struct InterchangeEnvelope {
    /// UNB segment (interchange header)
    pub unb: UnbSegment,
    /// UNZ segment (interchange trailer) - None until parsed/generated
    pub unz: Option<UnzSegment>,
    /// Messages within this interchange
    pub messages: Vec<MessageEnvelope>,
}

/// Message envelope containing a single EDIFACT message
#[derive(Debug, Clone)]
pub struct MessageEnvelope {
    /// UNH segment (message header)
    pub unh: UnhSegment,
    /// UNT segment (message trailer) - None until parsed/generated
    pub unt: Option<UntSegment>,
    /// Segments within this message (excluding envelope segments)
    pub segments: Vec<Segment>,
}

/// Syntax identifier for the interchange
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxIdentifier {
    /// Syntax identifier (e.g., "UNOA", "UNOB", "UNOC")
    pub identifier: String,
    /// Syntax version number (e.g., "1", "2", "3", "4")
    pub version: String,
    /// Service code list directory version (optional)
    pub service_code_list: Option<String>,
    /// Character encoding (optional, Coded character set)
    pub encoding: Option<String>,
}

/// Party identifier (sender or receiver)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartyId {
    /// Party identification (e.g., "SENDER001")
    pub id: String,
    /// Code qualifier (e.g., "14" for EAN International)
    pub qualifier: Option<String>,
    /// Internal identification (optional)
    pub internal_id: Option<String>,
    /// Internal qualifier (optional)
    pub internal_qualifier: Option<String>,
}

/// Date and time for interchange
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DateTime {
    /// Date in YYMMDD or CCYYMMDD format
    pub date: String,
    /// Time in HHMM or HHMMSS format
    pub time: String,
}

/// UNB - Interchange Header segment
#[derive(Debug, Clone)]
pub struct UnbSegment {
    /// Syntax identifier
    pub syntax_identifier: SyntaxIdentifier,
    /// Sender identification
    pub sender: PartyId,
    /// Receiver identification
    pub receiver: PartyId,
    /// Date and time of preparation
    pub datetime: DateTime,
    /// Interchange control reference
    pub control_ref: String,
    /// Application reference (optional)
    pub application_ref: Option<String>,
    /// Processing priority code (optional, e.g., "A" for highest priority)
    pub priority: Option<String>,
    /// Acknowledgement request (optional, "1" = request, "2" = no request)
    pub ack_request: Option<String>,
    /// Communications agreement ID (optional)
    pub comms_agreement_id: Option<String>,
    /// Test indicator (optional, "1" = test, empty = production)
    pub test_indicator: Option<String>,
}

/// UNZ - Interchange Trailer segment
#[derive(Debug, Clone)]
pub struct UnzSegment {
    /// Count of messages in interchange
    pub message_count: usize,
    /// Interchange control reference (must match UNB)
    pub control_ref: String,
}

/// Message type identifier (composite in UNH)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageTypeIdentifier {
    /// Message type (e.g., "ORDERS", "DESADV", "INVOIC")
    pub message_type: String,
    /// Message version number (e.g., "D")
    pub version: String,
    /// Message release number (e.g., "96A", "01B")
    pub release: String,
    /// Controlling agency (e.g., "UN" for UN/EDIFACT, "EAN" for EANCOM)
    pub agency: String,
    /// Association assigned code (optional)
    pub association_code: Option<String>,
}

/// UNH - Message Header segment
#[derive(Debug, Clone)]
pub struct UnhSegment {
    /// Message reference number
    pub message_ref: String,
    /// Message type identifier
    pub message_type: MessageTypeIdentifier,
    /// Common access reference (optional)
    pub common_access_ref: Option<String>,
    /// Status of transfer (optional - sequence of numbers)
    pub transfer_status: Option<Vec<String>>,
    /// Message subset identification (optional)
    pub subset_id: Option<String>,
    /// Implementation guideline identification (optional)
    pub implementation_id: Option<String>,
    /// Scenario identification (optional)
    pub scenario_id: Option<String>,
}

/// UNT - Message Trailer segment
#[derive(Debug, Clone)]
pub struct UntSegment {
    /// Number of segments in message (including UNH and UNT)
    pub segment_count: usize,
    /// Message reference number (must match UNH)
    pub message_ref: String,
}

/// UNA - Service String Advice
#[derive(Debug, Clone)]
pub struct UnaSegment {
    /// Component data element separator
    pub component_separator: u8,
    /// Data element separator
    pub element_separator: u8,
    /// Decimal notation
    pub decimal_point: u8,
    /// Release indicator
    pub release_character: u8,
    /// Reserved (space character)
    pub reserved: u8,
    /// Segment terminator
    pub segment_terminator: u8,
}

impl Default for UnaSegment {
    fn default() -> Self {
        Self {
            component_separator: b':',
            element_separator: b'+',
            decimal_point: b'.',
            release_character: b'?',
            reserved: b' ',
            segment_terminator: b'\'',
        }
    }
}

impl UnaSegment {
    /// Convert UNA to Separators struct
    pub fn to_separators(&self) -> crate::syntax::Separators {
        crate::syntax::Separators {
            component: self.component_separator,
            element: self.element_separator,
            decimal: self.decimal_point,
            release: self.release_character,
            segment: self.segment_terminator,
        }
    }

    /// Create UNA from Separators
    pub fn from_separators(sep: crate::syntax::Separators) -> Self {
        Self {
            component_separator: sep.component,
            element_separator: sep.element,
            decimal_point: sep.decimal,
            release_character: sep.release,
            reserved: b' ',
            segment_terminator: sep.segment,
        }
    }

    /// Serialize UNA to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        vec![
            b'U',
            b'N',
            b'A',
            self.component_separator,
            self.element_separator,
            self.decimal_point,
            self.release_character,
            self.reserved,
            self.segment_terminator,
        ]
    }
}

impl Default for SyntaxIdentifier {
    fn default() -> Self {
        Self {
            identifier: "UNOA".to_string(),
            version: "3".to_string(),
            service_code_list: None,
            encoding: None,
        }
    }
}

impl Default for MessageTypeIdentifier {
    fn default() -> Self {
        Self {
            message_type: "ORDERS".to_string(),
            version: "D".to_string(),
            release: "96A".to_string(),
            agency: "UN".to_string(),
            association_code: None,
        }
    }
}

// ============================================================================
// Parsing Functions
// ============================================================================

/// Parse a UNB (Interchange Header) segment
pub fn parse_unb(segment: &Segment) -> Result<UnbSegment> {
    if segment.tag != "UNB" {
        return Err(Error::Envelope(format!(
            "Expected UNB segment, got {}",
            segment.tag
        )));
    }

    // UNB has minimum 5 elements
    if segment.elements.len() < 5 {
        return Err(Error::Envelope(format!(
            "UNB segment must have at least 5 elements, got {}",
            segment.elements.len()
        )));
    }

    // Parse syntax identifier (element 0 - composite)
    let syntax_identifier = parse_syntax_identifier(&segment.elements[0])?;

    // Parse sender (element 1 - composite)
    let sender = parse_party_id(&segment.elements[1])?;

    // Parse receiver (element 2 - composite)
    let receiver = parse_party_id(&segment.elements[2])?;

    // Parse datetime (element 3 - composite)
    let datetime = parse_datetime(&segment.elements[3])?;

    // Parse control reference (element 4 - simple)
    let control_ref = parse_simple_string(&segment.elements[4], "control reference")?;

    // Parse optional fields
    let application_ref = segment
        .elements
        .get(5)
        .and_then(|e| parse_simple_string(e, "application reference").ok());

    let priority = segment
        .elements
        .get(6)
        .and_then(|e| parse_simple_string(e, "priority").ok());

    let ack_request = segment
        .elements
        .get(7)
        .and_then(|e| parse_simple_string(e, "ack request").ok());

    let comms_agreement_id = segment
        .elements
        .get(8)
        .and_then(|e| parse_simple_string(e, "comms agreement").ok());

    let test_indicator = segment
        .elements
        .get(9)
        .and_then(|e| parse_simple_string(e, "test indicator").ok());

    Ok(UnbSegment {
        syntax_identifier,
        sender,
        receiver,
        datetime,
        control_ref,
        application_ref,
        priority,
        ack_request,
        comms_agreement_id,
        test_indicator,
    })
}

/// Parse a UNZ (Interchange Trailer) segment
pub fn parse_unz(segment: &Segment) -> Result<UnzSegment> {
    if segment.tag != "UNZ" {
        return Err(Error::Envelope(format!(
            "Expected UNZ segment, got {}",
            segment.tag
        )));
    }

    if segment.elements.len() < 2 {
        return Err(Error::Envelope(
            "UNZ segment must have at least 2 elements".to_string(),
        ));
    }

    let message_count = parse_simple_usize(&segment.elements[0], "message count")?;
    let control_ref = parse_simple_string(&segment.elements[1], "control reference")?;

    Ok(UnzSegment {
        message_count,
        control_ref,
    })
}

/// Parse a UNH (Message Header) segment
pub fn parse_unh(segment: &Segment) -> Result<UnhSegment> {
    if segment.tag != "UNH" {
        return Err(Error::Envelope(format!(
            "Expected UNH segment, got {}",
            segment.tag
        )));
    }

    if segment.elements.len() < 2 {
        return Err(Error::Envelope(
            "UNH segment must have at least 2 elements".to_string(),
        ));
    }

    let message_ref = parse_simple_string(&segment.elements[0], "message reference")?;
    let message_type = parse_message_type_identifier(&segment.elements[1])?;

    // Parse optional fields
    let common_access_ref = segment
        .elements
        .get(2)
        .and_then(|e| parse_simple_string(e, "common access ref").ok());

    let transfer_status = segment.elements.get(3).and_then(|e| {
        if let Element::Composite(comps) = e {
            Some(
                comps
                    .iter()
                    .map(|c| String::from_utf8_lossy(c).to_string())
                    .collect(),
            )
        } else if let Element::Simple(val) = e {
            Some(vec![String::from_utf8_lossy(val).to_string()])
        } else {
            None
        }
    });

    let subset_id = segment
        .elements
        .get(4)
        .and_then(|e| parse_simple_string(e, "subset id").ok());

    let implementation_id = segment
        .elements
        .get(5)
        .and_then(|e| parse_simple_string(e, "implementation id").ok());

    let scenario_id = segment
        .elements
        .get(6)
        .and_then(|e| parse_simple_string(e, "scenario id").ok());

    Ok(UnhSegment {
        message_ref,
        message_type,
        common_access_ref,
        transfer_status,
        subset_id,
        implementation_id,
        scenario_id,
    })
}

/// Parse a UNT (Message Trailer) segment
pub fn parse_unt(segment: &Segment) -> Result<UntSegment> {
    if segment.tag != "UNT" {
        return Err(Error::Envelope(format!(
            "Expected UNT segment, got {}",
            segment.tag
        )));
    }

    if segment.elements.len() < 2 {
        return Err(Error::Envelope(
            "UNT segment must have at least 2 elements".to_string(),
        ));
    }

    let segment_count = parse_simple_usize(&segment.elements[0], "segment count")?;
    let message_ref = parse_simple_string(&segment.elements[1], "message reference")?;

    Ok(UntSegment {
        segment_count,
        message_ref,
    })
}

/// Parse UNA (Service String Advice) from raw bytes
pub fn parse_una(data: &[u8]) -> Result<UnaSegment> {
    if data.len() < 9 {
        return Err(Error::Envelope(
            "UNA segment must be at least 9 bytes".to_string(),
        ));
    }

    if &data[0..3] != b"UNA" {
        return Err(Error::Envelope(
            "UNA segment must start with 'UNA'".to_string(),
        ));
    }

    Ok(UnaSegment {
        component_separator: data[3],
        element_separator: data[4],
        decimal_point: data[5],
        release_character: data[6],
        reserved: data[7],
        segment_terminator: data[8],
    })
}

// ============================================================================
// Helper parsing functions
// ============================================================================

fn parse_syntax_identifier(element: &Element) -> Result<SyntaxIdentifier> {
    match element {
        Element::Composite(comps) => {
            if comps.is_empty() {
                return Err(Error::Envelope(
                    "Syntax identifier must have at least identifier and version".to_string(),
                ));
            }

            let identifier = String::from_utf8_lossy(&comps[0]).to_string();
            let version = comps
                .get(1)
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_default();
            let service_code_list = comps
                .get(2)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());
            let encoding = comps
                .get(3)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());

            Ok(SyntaxIdentifier {
                identifier,
                version,
                service_code_list,
                encoding,
            })
        }
        Element::Simple(_) => {
            // Simple element not valid for syntax identifier
            Err(Error::Envelope(
                "Syntax identifier must be composite".to_string(),
            ))
        }
    }
}

fn parse_party_id(element: &Element) -> Result<PartyId> {
    match element {
        Element::Composite(comps) => {
            if comps.is_empty() {
                return Err(Error::Envelope(
                    "Party ID must have at least the ID".to_string(),
                ));
            }

            let id = String::from_utf8_lossy(&comps[0]).to_string();
            let qualifier = comps
                .get(1)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());
            let internal_id = comps
                .get(2)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());
            let internal_qualifier = comps
                .get(3)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());

            Ok(PartyId {
                id,
                qualifier,
                internal_id,
                internal_qualifier,
            })
        }
        Element::Simple(val) => {
            // Single value party ID (just the ID, no qualifier)
            Ok(PartyId {
                id: String::from_utf8_lossy(val).to_string(),
                qualifier: None,
                internal_id: None,
                internal_qualifier: None,
            })
        }
    }
}

fn parse_datetime(element: &Element) -> Result<DateTime> {
    match element {
        Element::Composite(comps) => {
            if comps.len() < 2 {
                return Err(Error::Envelope(
                    "DateTime must have date and time components".to_string(),
                ));
            }

            Ok(DateTime {
                date: String::from_utf8_lossy(&comps[0]).to_string(),
                time: String::from_utf8_lossy(&comps[1]).to_string(),
            })
        }
        Element::Simple(_) => Err(Error::Envelope("DateTime must be composite".to_string())),
    }
}

fn parse_message_type_identifier(element: &Element) -> Result<MessageTypeIdentifier> {
    match element {
        Element::Composite(comps) => {
            if comps.len() < 4 {
                return Err(Error::Envelope(
                    "Message type identifier must have type, version, release, and agency"
                        .to_string(),
                ));
            }

            let message_type = String::from_utf8_lossy(&comps[0]).to_string();
            let version = String::from_utf8_lossy(&comps[1]).to_string();
            let release = String::from_utf8_lossy(&comps[2]).to_string();
            let agency = String::from_utf8_lossy(&comps[3]).to_string();
            let association_code = comps
                .get(4)
                .filter(|v| !v.is_empty())
                .map(|v| String::from_utf8_lossy(v).to_string());

            Ok(MessageTypeIdentifier {
                message_type,
                version,
                release,
                agency,
                association_code,
            })
        }
        Element::Simple(_) => Err(Error::Envelope(
            "Message type identifier must be composite".to_string(),
        )),
    }
}

fn parse_simple_string(element: &Element, field_name: &str) -> Result<String> {
    match element {
        Element::Simple(val) => Ok(String::from_utf8_lossy(val).to_string()),
        Element::Composite(comps) if comps.len() == 1 => {
            Ok(String::from_utf8_lossy(&comps[0]).to_string())
        }
        _ => Err(Error::Envelope(format!(
            "Expected simple value for {}, got composite",
            field_name
        ))),
    }
}

fn parse_simple_usize(element: &Element, field_name: &str) -> Result<usize> {
    let s = parse_simple_string(element, field_name)?;
    s.parse::<usize>()
        .map_err(|_| Error::Envelope(format!("Invalid numeric value for {}: {}", field_name, s)))
}

// ============================================================================
// Generation Functions
// ============================================================================

/// Generate a UNB segment from UnbSegment
pub fn generate_unb(unb: &UnbSegment, separators: &crate::syntax::Separators) -> Segment {
    let mut elements = Vec::new();

    // Syntax identifier (composite)
    let mut syntax_comps = vec![
        unb.syntax_identifier.identifier.as_bytes().to_vec(),
        unb.syntax_identifier.version.as_bytes().to_vec(),
    ];
    if let Some(ref scl) = unb.syntax_identifier.service_code_list {
        syntax_comps.push(scl.as_bytes().to_vec());
        if let Some(ref enc) = unb.syntax_identifier.encoding {
            syntax_comps.push(enc.as_bytes().to_vec());
        }
    } else if unb.syntax_identifier.encoding.is_some() {
        syntax_comps.push(vec![]); // Empty service code list
        syntax_comps.push(
            unb.syntax_identifier
                .encoding
                .as_ref()
                .unwrap()
                .as_bytes()
                .to_vec(),
        );
    }
    elements.push(Element::Composite(syntax_comps));

    // Sender (composite)
    let mut sender_comps = vec![unb.sender.id.as_bytes().to_vec()];
    if let Some(ref q) = unb.sender.qualifier {
        sender_comps.push(q.as_bytes().to_vec());
        if let Some(ref ii) = unb.sender.internal_id {
            sender_comps.push(ii.as_bytes().to_vec());
            if let Some(ref iq) = unb.sender.internal_qualifier {
                sender_comps.push(iq.as_bytes().to_vec());
            }
        }
    }
    elements.push(Element::Composite(sender_comps));

    // Receiver (composite)
    let mut receiver_comps = vec![unb.receiver.id.as_bytes().to_vec()];
    if let Some(ref q) = unb.receiver.qualifier {
        receiver_comps.push(q.as_bytes().to_vec());
        if let Some(ref ii) = unb.receiver.internal_id {
            receiver_comps.push(ii.as_bytes().to_vec());
            if let Some(ref iq) = unb.receiver.internal_qualifier {
                receiver_comps.push(iq.as_bytes().to_vec());
            }
        }
    }
    elements.push(Element::Composite(receiver_comps));

    // DateTime (composite)
    elements.push(Element::Composite(vec![
        unb.datetime.date.as_bytes().to_vec(),
        unb.datetime.time.as_bytes().to_vec(),
    ]));

    // Control reference
    elements.push(Element::Simple(unb.control_ref.as_bytes().to_vec()));

    // Optional fields
    if let Some(ref ar) = unb.application_ref {
        elements.push(Element::Simple(ar.as_bytes().to_vec()));
    } else {
        return create_segment("UNB", elements, separators);
    }

    if let Some(ref p) = unb.priority {
        elements.push(Element::Simple(p.as_bytes().to_vec()));
    } else {
        return create_segment("UNB", elements, separators);
    }

    if let Some(ref ar) = unb.ack_request {
        elements.push(Element::Simple(ar.as_bytes().to_vec()));
    } else {
        return create_segment("UNB", elements, separators);
    }

    if let Some(ref cai) = unb.comms_agreement_id {
        elements.push(Element::Simple(cai.as_bytes().to_vec()));
    } else {
        return create_segment("UNB", elements, separators);
    }

    if let Some(ref ti) = unb.test_indicator {
        elements.push(Element::Simple(ti.as_bytes().to_vec()));
    }

    create_segment("UNB", elements, separators)
}

/// Generate a UNZ segment from UnzSegment
pub fn generate_unz(unz: &UnzSegment, separators: &crate::syntax::Separators) -> Segment {
    let elements = vec![
        Element::Simple(unz.message_count.to_string().as_bytes().to_vec()),
        Element::Simple(unz.control_ref.as_bytes().to_vec()),
    ];
    create_segment("UNZ", elements, separators)
}

/// Generate a UNH segment from UnhSegment
pub fn generate_unh(unh: &UnhSegment, separators: &crate::syntax::Separators) -> Segment {
    let mut elements = Vec::new();

    // Message reference
    elements.push(Element::Simple(unh.message_ref.as_bytes().to_vec()));

    // Message type identifier (composite)
    let mut msg_type_comps = vec![
        unh.message_type.message_type.as_bytes().to_vec(),
        unh.message_type.version.as_bytes().to_vec(),
        unh.message_type.release.as_bytes().to_vec(),
        unh.message_type.agency.as_bytes().to_vec(),
    ];
    if let Some(ref ac) = unh.message_type.association_code {
        msg_type_comps.push(ac.as_bytes().to_vec());
    }
    elements.push(Element::Composite(msg_type_comps));

    // Optional fields
    if let Some(ref car) = unh.common_access_ref {
        elements.push(Element::Simple(car.as_bytes().to_vec()));
    } else {
        return create_segment("UNH", elements, separators);
    }

    if let Some(ref ts) = unh.transfer_status {
        let comps: Vec<Vec<u8>> = ts.iter().map(|s| s.as_bytes().to_vec()).collect();
        elements.push(Element::Composite(comps));
    } else {
        return create_segment("UNH", elements, separators);
    }

    if let Some(ref si) = unh.subset_id {
        elements.push(Element::Simple(si.as_bytes().to_vec()));
    } else {
        return create_segment("UNH", elements, separators);
    }

    if let Some(ref ii) = unh.implementation_id {
        elements.push(Element::Simple(ii.as_bytes().to_vec()));
    } else {
        return create_segment("UNH", elements, separators);
    }

    if let Some(ref si) = unh.scenario_id {
        elements.push(Element::Simple(si.as_bytes().to_vec()));
    }

    create_segment("UNH", elements, separators)
}

/// Generate a UNT segment from UntSegment
pub fn generate_unt(unt: &UntSegment, separators: &crate::syntax::Separators) -> Segment {
    let elements = vec![
        Element::Simple(unt.segment_count.to_string().as_bytes().to_vec()),
        Element::Simple(unt.message_ref.as_bytes().to_vec()),
    ];
    create_segment("UNT", elements, separators)
}

fn create_segment(
    tag: &str,
    elements: Vec<Element>,
    _separators: &crate::syntax::Separators,
) -> Segment {
    Segment {
        tag: tag.to_string(),
        elements,
        position: Position::default(),
    }
}

// ============================================================================
// Validation Functions
// ============================================================================

/// Validate an interchange envelope
pub fn validate_interchange(interchange: &InterchangeEnvelope) -> Result<()> {
    // Check UNZ exists
    let unz = interchange
        .unz
        .as_ref()
        .ok_or_else(|| Error::Envelope("Missing UNZ segment".to_string()))?;

    // Check control reference matching
    if unz.control_ref != interchange.unb.control_ref {
        return Err(Error::Envelope(format!(
            "Interchange control reference mismatch: UNB='{}', UNZ='{}'",
            interchange.unb.control_ref, unz.control_ref
        )));
    }

    // Check message count
    if unz.message_count != interchange.messages.len() {
        return Err(Error::Envelope(format!(
            "Message count mismatch: UNZ says {}, actual count is {}",
            unz.message_count,
            interchange.messages.len()
        )));
    }

    // Validate each message
    for (i, message) in interchange.messages.iter().enumerate() {
        validate_message(message)
            .map_err(|e| Error::Envelope(format!("Message {} validation failed: {}", i + 1, e)))?;
    }

    Ok(())
}

/// Validate a message envelope
pub fn validate_message(message: &MessageEnvelope) -> Result<()> {
    // Check UNT exists
    let unt = message
        .unt
        .as_ref()
        .ok_or_else(|| Error::Envelope("Missing UNT segment".to_string()))?;

    // Check message reference matching
    if unt.message_ref != message.unh.message_ref {
        return Err(Error::Envelope(format!(
            "Message reference mismatch: UNH='{}', UNT='{}'",
            message.unh.message_ref, unt.message_ref
        )));
    }

    // Check segment count (including UNH and UNT)
    let expected_count = message.segments.len() + 2; // +2 for UNH and UNT
    if unt.segment_count != expected_count {
        return Err(Error::Envelope(format!(
            "Segment count mismatch: UNT says {}, actual count is {} (including UNH/UNT)",
            unt.segment_count, expected_count
        )));
    }

    Ok(())
}

// ============================================================================
// Control Number Management
// ============================================================================

/// Trait for generating control numbers
pub trait ControlNumberGenerator: Send + Sync {
    /// Generate the next interchange control reference
    fn next_interchange_ref(&self) -> Result<String>;
    /// Generate the next message reference for a given interchange
    fn next_message_ref(&self, interchange_ref: &str) -> Result<String>;
    /// Reset counters for testing
    fn reset(&self) -> Result<()>;
}

/// File-based control number generator that persists sequences to disk
#[derive(Debug)]
pub struct FileBasedControlNumberGenerator {
    file_path: String,
    state: Arc<Mutex<ControlNumberState>>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ControlNumberState {
    interchange_counter: u64,
    message_counters: HashMap<String, u64>,
}

impl FileBasedControlNumberGenerator {
    /// Create a new file-based generator
    pub fn new(file_path: impl Into<String>) -> Result<Self> {
        let file_path = file_path.into();
        let state = if Path::new(&file_path).exists() {
            let contents = fs::read_to_string(&file_path)?;
            serde_json::from_str(&contents).unwrap_or_default()
        } else {
            ControlNumberState::default()
        };

        Ok(Self {
            file_path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Persist current state to disk
    pub fn save(&self) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        let json = serde_json::to_string_pretty(&*state)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        fs::write(&self.file_path, json)?;
        Ok(())
    }
}

impl ControlNumberGenerator for FileBasedControlNumberGenerator {
    fn next_interchange_ref(&self) -> Result<String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        state.interchange_counter += 1;
        let ref_num = format!("{:014}", state.interchange_counter);
        drop(state);
        self.save()?;
        Ok(ref_num)
    }

    fn next_message_ref(&self, interchange_ref: &str) -> Result<String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        let counter = state
            .message_counters
            .entry(interchange_ref.to_string())
            .or_insert(0);
        *counter += 1;
        let ref_num = format!("{:06}", *counter);
        drop(state);
        self.save()?;
        Ok(ref_num)
    }

    fn reset(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        state.interchange_counter = 0;
        state.message_counters.clear();
        drop(state);
        self.save()?;
        Ok(())
    }
}

/// Memory-based control number generator for testing
#[derive(Debug, Default)]
pub struct MemoryControlNumberGenerator {
    state: Arc<Mutex<ControlNumberState>>,
}

impl MemoryControlNumberGenerator {
    /// Create a new memory-based generator
    pub fn new() -> Self {
        Self::default()
    }
}

impl ControlNumberGenerator for MemoryControlNumberGenerator {
    fn next_interchange_ref(&self) -> Result<String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        state.interchange_counter += 1;
        Ok(format!("{:014}", state.interchange_counter))
    }

    fn next_message_ref(&self, interchange_ref: &str) -> Result<String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        let counter = state
            .message_counters
            .entry(interchange_ref.to_string())
            .or_insert(0);
        *counter += 1;
        Ok(format!("{:06}", *counter))
    }

    fn reset(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Envelope("Failed to lock state".to_string()))?;
        state.interchange_counter = 0;
        state.message_counters.clear();
        Ok(())
    }
}

// ============================================================================
// Envelope Validator
// ============================================================================

/// Type of envelope being validated
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeType {
    /// Interchange envelope (UNB/UNZ)
    Interchange,
    /// Message envelope (UNH/UNT)
    Message,
}

impl std::fmt::Display for EnvelopeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeType::Interchange => write!(f, "Interchange"),
            EnvelopeType::Message => write!(f, "Message"),
        }
    }
}

/// Kind of validation error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Missing header segment (UNB or UNH)
    MissingHeader,
    /// Missing trailer segment (UNZ or UNT)
    MissingTrailer,
    /// Control reference mismatch between header and trailer
    MismatchedControlReference,
    /// Incorrect segment count in UNT
    IncorrectSegmentCount,
    /// Incorrect message count in UNZ
    IncorrectMessageCount,
    /// Invalid syntax identifier in UNB
    InvalidSyntaxIdentifier,
    /// Invalid message type in UNH
    InvalidMessageType,
    /// Message outside of interchange
    OrphanedMessage,
    /// Segment outside of message
    OrphanedSegment,
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::MissingHeader => write!(f, "missing header"),
            ErrorKind::MissingTrailer => write!(f, "missing trailer"),
            ErrorKind::MismatchedControlReference => write!(f, "mismatched control reference"),
            ErrorKind::IncorrectSegmentCount => write!(f, "incorrect segment count"),
            ErrorKind::IncorrectMessageCount => write!(f, "incorrect message count"),
            ErrorKind::InvalidSyntaxIdentifier => write!(f, "invalid syntax identifier"),
            ErrorKind::InvalidMessageType => write!(f, "invalid message type"),
            ErrorKind::OrphanedMessage => write!(f, "message outside of interchange"),
            ErrorKind::OrphanedSegment => write!(f, "segment outside of message"),
        }
    }
}

/// Validation error with position information
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Human-readable error message
    pub message: String,
    /// Type of envelope
    pub envelope_type: EnvelopeType,
    /// Position in source
    pub position: Position,
    /// Kind of error
    pub error_kind: ErrorKind,
    /// Expected value (if applicable)
    pub expected: Option<String>,
    /// Actual value (if applicable)
    pub actual: Option<String>,
}

impl ValidationError {
    /// Create a new validation error
    pub fn new(
        message: impl Into<String>,
        envelope_type: EnvelopeType,
        position: Position,
        error_kind: ErrorKind,
    ) -> Self {
        Self {
            message: message.into(),
            envelope_type,
            position,
            error_kind,
            expected: None,
            actual: None,
        }
    }

    /// Add expected and actual values
    pub fn with_values(mut self, expected: impl Into<String>, actual: impl Into<String>) -> Self {
        self.expected = Some(expected.into());
        self.actual = Some(actual.into());
        self
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} validation error at line {}: {} (kind: {:?})",
            self.envelope_type, self.position.line, self.message, self.error_kind
        )?;
        if let (Some(expected), Some(actual)) = (&self.expected, &self.actual) {
            write!(f, " [expected: {}, actual: {}]", expected, actual)?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationError {}

/// Validation warning (non-fatal)
#[derive(Debug, Clone)]
pub struct ValidationWarning {
    /// Warning message
    pub message: String,
    /// Type of envelope
    pub envelope_type: EnvelopeType,
    /// Position in source
    pub position: Position,
}

impl ValidationWarning {
    /// Create a new validation warning
    pub fn new(
        message: impl Into<String>,
        envelope_type: EnvelopeType,
        position: Position,
    ) -> Self {
        Self {
            message: message.into(),
            envelope_type,
            position,
        }
    }
}

/// Validation report containing errors and warnings
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Whether the document is valid (no errors)
    pub is_valid: bool,
    /// List of validation errors
    pub errors: Vec<ValidationError>,
    /// List of validation warnings
    pub warnings: Vec<ValidationWarning>,
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }
}

impl ValidationReport {
    /// Create a new empty validation report
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an error to the report
    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    /// Add a warning to the report
    pub fn add_warning(&mut self, warning: ValidationWarning) {
        self.warnings.push(warning);
    }

    /// Merge another report into this one
    pub fn merge(&mut self, other: ValidationReport) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
        self.is_valid = self.is_valid && other.is_valid;
    }

    /// Get the number of errors
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Get the number of warnings
    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }
}

/// Document structure for validation
///
/// This represents a parsed EDIFACT document with interchange and message envelopes
#[derive(Debug, Clone)]
pub struct EdifactDocument {
    /// Optional UNA segment
    pub una: Option<UnaSegment>,
    /// Interchange envelope (UNB/UNZ pair)
    pub interchange: Option<InterchangeEnvelope>,
}

impl EdifactDocument {
    /// Create a new empty document
    pub fn new() -> Self {
        Self {
            una: None,
            interchange: None,
        }
    }

    /// Check if the document has an interchange envelope
    pub fn has_interchange(&self) -> bool {
        self.interchange.is_some()
    }

    /// Get the number of messages in the document
    pub fn message_count(&self) -> usize {
        self.interchange
            .as_ref()
            .map(|i| i.messages.len())
            .unwrap_or(0)
    }
}

impl Default for EdifactDocument {
    fn default() -> Self {
        Self::new()
    }
}

/// Validator for EDIFACT envelope pairings
///
/// Validates that:
/// - UNB/UNZ pairs match (control reference, message count)
/// - UNH/UNT pairs match (message reference, segment count)
/// - Messages are properly nested within interchanges
/// - Segments are properly nested within messages
#[derive(Debug, Clone, Default)]
pub struct EnvelopeValidator;

impl EnvelopeValidator {
    /// Create a new envelope validator
    pub fn new() -> Self {
        Self
    }

    /// Validate all envelope pairings in a document
    ///
    /// This performs comprehensive validation of the entire document structure,
    /// checking both interchange-level (UNB/UNZ) and message-level (UNH/UNT) pairings.
    ///
    /// # Arguments
    /// * `document` - The EDIFACT document to validate
    ///
    /// # Returns
    /// A `ValidationReport` containing all errors and warnings found
    pub fn validate(
        &self,
        document: &EdifactDocument,
    ) -> std::result::Result<ValidationReport, Error> {
        let mut report = ValidationReport::new();

        // Validate interchange level
        if let Some(ref interchange) = document.interchange {
            match self.validate_interchange(interchange) {
                Ok(()) => {}
                Err(e) => report.add_error(e),
            }

            // Validate each message
            for (idx, message) in interchange.messages.iter().enumerate() {
                match self.validate_message(message) {
                    Ok(()) => {}
                    Err(e) => {
                        let error = ValidationError::new(
                            format!("Message {} validation failed: {}", idx + 1, e.message),
                            EnvelopeType::Message,
                            e.position,
                            e.error_kind,
                        )
                        .with_values(e.expected.unwrap_or_default(), e.actual.unwrap_or_default());
                        report.add_error(error);
                    }
                }
            }
        } else {
            report.add_error(ValidationError::new(
                "Missing interchange envelope (UNB)",
                EnvelopeType::Interchange,
                Position::default(),
                ErrorKind::MissingHeader,
            ));
        }

        Ok(report)
    }

    /// Validate UNB/UNZ pairing for an interchange
    ///
    /// Checks:
    /// - UNZ exists
    /// - Control references match
    /// - Message count matches actual
    pub fn validate_interchange(
        &self,
        interchange: &InterchangeEnvelope,
    ) -> std::result::Result<(), ValidationError> {
        // Check UNZ exists
        let unz = interchange.unz.as_ref().ok_or_else(|| {
            ValidationError::new(
                "Missing UNZ segment (interchange trailer)",
                EnvelopeType::Interchange,
                Position::default(),
                ErrorKind::MissingTrailer,
            )
        })?;

        // Check control reference matching
        if unz.control_ref != interchange.unb.control_ref {
            return Err(ValidationError::new(
                format!(
                    "Interchange control reference mismatch: UNB='{}', UNZ='{}'",
                    interchange.unb.control_ref, unz.control_ref
                ),
                EnvelopeType::Interchange,
                Position::default(),
                ErrorKind::MismatchedControlReference,
            )
            .with_values(&interchange.unb.control_ref, &unz.control_ref));
        }

        // Check message count
        if unz.message_count != interchange.messages.len() {
            return Err(ValidationError::new(
                format!(
                    "Message count mismatch: UNZ declares {}, actual count is {}",
                    unz.message_count,
                    interchange.messages.len()
                ),
                EnvelopeType::Interchange,
                Position::default(),
                ErrorKind::IncorrectMessageCount,
            )
            .with_values(
                unz.message_count.to_string(),
                interchange.messages.len().to_string(),
            ));
        }

        // Validate syntax identifier
        self.validate_syntax_identifier(&interchange.unb.syntax_identifier)?;

        Ok(())
    }

    /// Validate UNH/UNT pairing for a message
    ///
    /// Checks:
    /// - UNT exists
    /// - Message references match
    /// - Segment count matches actual
    /// - Message type is valid
    pub fn validate_message(
        &self,
        message: &MessageEnvelope,
    ) -> std::result::Result<(), ValidationError> {
        // Check UNT exists
        let unt = message.unt.as_ref().ok_or_else(|| {
            ValidationError::new(
                format!(
                    "Missing UNT segment for message '{}'(message trailer)",
                    message.unh.message_ref
                ),
                EnvelopeType::Message,
                Position::default(),
                ErrorKind::MissingTrailer,
            )
        })?;

        // Check message reference matching
        if unt.message_ref != message.unh.message_ref {
            return Err(ValidationError::new(
                format!(
                    "Message reference mismatch: UNH='{}', UNT='{}'",
                    message.unh.message_ref, unt.message_ref
                ),
                EnvelopeType::Message,
                Position::default(),
                ErrorKind::MismatchedControlReference,
            )
            .with_values(&message.unh.message_ref, &unt.message_ref));
        }

        // Check segment count (including UNH and UNT)
        let expected_count = message.segments.len() + 2; // +2 for UNH and UNT
        if unt.segment_count != expected_count {
            return Err(ValidationError::new(
                format!(
                    "Segment count mismatch: UNT declares {}, actual count is {} (including UNH/UNT)",
                    unt.segment_count, expected_count
                ),
                EnvelopeType::Message,
                Position::default(),
                ErrorKind::IncorrectSegmentCount,
            )
            .with_values(
                unt.segment_count.to_string(),
                expected_count.to_string(),
            ));
        }

        // Validate message type
        self.validate_message_type(&message.unh.message_type)?;

        Ok(())
    }

    /// Check control reference matches between header and trailer segments
    ///
    /// This is a lower-level method that works directly with Segment structs
    /// for use during parsing.
    pub fn check_control_reference(
        &self,
        header: &Segment,
        trailer: &Segment,
    ) -> std::result::Result<(), Error> {
        // Extract control reference from header (UNB.0020 or UNH.0062)
        let header_ref = match header.tag.as_str() {
            "UNB" => header
                .elements
                .get(4)
                .ok_or_else(|| Error::Envelope("UNB missing control reference element".to_string()))
                .and_then(|e| match e {
                    Element::Simple(v) => Ok(String::from_utf8_lossy(v).to_string()),
                    Element::Composite(c) if !c.is_empty() => {
                        Ok(String::from_utf8_lossy(&c[0]).to_string())
                    }
                    _ => Err(Error::Envelope(
                        "UNB control reference has unexpected format".to_string(),
                    )),
                }),
            "UNH" => header
                .elements
                .first()
                .ok_or_else(|| Error::Envelope("UNH missing message reference element".to_string()))
                .and_then(|e| match e {
                    Element::Simple(v) => Ok(String::from_utf8_lossy(v).to_string()),
                    Element::Composite(c) if !c.is_empty() => {
                        Ok(String::from_utf8_lossy(&c[0]).to_string())
                    }
                    _ => Err(Error::Envelope(
                        "UNH message reference has unexpected format".to_string(),
                    )),
                }),
            _ => Err(Error::Envelope(format!(
                "Unsupported header segment: {}",
                header.tag
            ))),
        }?;

        // Extract control reference from trailer (UNZ.0036 or UNT.0062)
        let trailer_ref = match trailer.tag.as_str() {
            "UNZ" => trailer
                .elements
                .get(1)
                .ok_or_else(|| Error::Envelope("UNZ missing control reference element".to_string()))
                .and_then(|e| match e {
                    Element::Simple(v) => Ok(String::from_utf8_lossy(v).to_string()),
                    Element::Composite(c) if !c.is_empty() => {
                        Ok(String::from_utf8_lossy(&c[0]).to_string())
                    }
                    _ => Err(Error::Envelope(
                        "UNZ control reference has unexpected format".to_string(),
                    )),
                }),
            "UNT" => trailer
                .elements
                .get(1)
                .ok_or_else(|| Error::Envelope("UNT missing message reference element".to_string()))
                .and_then(|e| match e {
                    Element::Simple(v) => Ok(String::from_utf8_lossy(v).to_string()),
                    Element::Composite(c) if !c.is_empty() => {
                        Ok(String::from_utf8_lossy(&c[0]).to_string())
                    }
                    _ => Err(Error::Envelope(
                        "UNT message reference has unexpected format".to_string(),
                    )),
                }),
            _ => Err(Error::Envelope(format!(
                "Unsupported trailer segment: {}",
                trailer.tag
            ))),
        }?;

        if header_ref != trailer_ref {
            return Err(Error::Envelope(format!(
                "Control reference mismatch: header='{}', trailer='{}'",
                header_ref, trailer_ref
            )));
        }

        Ok(())
    }

    /// Check segment count matches in UNT segment
    ///
    /// This is a lower-level method that works with the message envelope
    /// and a parsed UNT segment.
    pub fn check_segment_count(
        &self,
        message: &MessageEnvelope,
        trailer: &Segment,
    ) -> std::result::Result<(), Error> {
        if trailer.tag != "UNT" {
            return Err(Error::Envelope(format!(
                "Expected UNT segment, got {}",
                trailer.tag
            )));
        }

        // Extract declared segment count from UNT
        let declared_count = trailer
            .elements
            .first()
            .ok_or_else(|| Error::Envelope("UNT missing segment count element".to_string()))
            .and_then(|e| match e {
                Element::Simple(v) => {
                    let s = String::from_utf8_lossy(v);
                    s.parse::<usize>()
                        .map_err(|_| Error::Envelope(format!("Invalid segment count: {}", s)))
                }
                Element::Composite(c) if !c.is_empty() => {
                    let s = String::from_utf8_lossy(&c[0]);
                    s.parse::<usize>()
                        .map_err(|_| Error::Envelope(format!("Invalid segment count: {}", s)))
                }
                _ => Err(Error::Envelope(
                    "UNT segment count has unexpected format".to_string(),
                )),
            })?;

        // Calculate actual segment count (including UNH and UNT)
        let actual_count = message.segments.len() + 2;

        if declared_count != actual_count {
            return Err(Error::Envelope(format!(
                "Segment count mismatch: UNT declares {}, actual is {}",
                declared_count, actual_count
            )));
        }

        Ok(())
    }

    /// Validate syntax identifier
    fn validate_syntax_identifier(
        &self,
        syntax: &SyntaxIdentifier,
    ) -> std::result::Result<(), ValidationError> {
        let valid_identifiers = [
            "UNOA", "UNOB", "UNOC", "UNOD", "UNOE", "UNOF", "UNOG", "UNOH", "UNOI", "UNOJ", "UNOK",
        ];

        if !valid_identifiers.contains(&syntax.identifier.as_str()) {
            return Err(ValidationError::new(
                format!("Invalid syntax identifier: '{}'", syntax.identifier),
                EnvelopeType::Interchange,
                Position::default(),
                ErrorKind::InvalidSyntaxIdentifier,
            ));
        }

        // Validate version (typically 1-4)
        if let Ok(version) = syntax.version.parse::<u8>() {
            if version == 0 || version > 4 {
                return Err(ValidationError::new(
                    format!("Invalid syntax version: '{}'", syntax.version),
                    EnvelopeType::Interchange,
                    Position::default(),
                    ErrorKind::InvalidSyntaxIdentifier,
                ));
            }
        }

        Ok(())
    }

    /// Validate message type identifier
    fn validate_message_type(
        &self,
        msg_type: &MessageTypeIdentifier,
    ) -> std::result::Result<(), ValidationError> {
        // List of common EDIFACT/EANCOM message types
        let valid_types = [
            "ORDERS", "ORDRSP", "DESADV", "INVOIC", "SLSRPT", "PRICAT", "RECADV", "REMADV",
            "INVRPT", "GENRAL", "CONTRL", "APERAK", "BANSTA", "HSTAT", "OSTENQ", "OSTRPT",
            "PARTIN", "PRODAT", "PROINQ", "QALITY", "QUOTES", "REQOTE", "SANCRT", "CONITT",
            "CONDRA", "CONQVY", "COPARN", "COPINO", "COPRAR", "COREOR", "COSHI", "COSMAT",
            "COSTCO", "COSTOR", "CUSCAR", "CUSDEC", "CUSEXP", "CUSRES", "DEBADV", "DEBMUL",
            "DELFOR", "DELJIT", "DESADV", "DGRECA", "DOCADV", "DOCAMA", "DOCAMI", "DOCAMR",
            "DOCAPP", "FINPAY", "FINSTA", "GENRAL", "HANMOV", "ICASRP", "ICSOLI", "IFCSUM",
            "IFTCCA", "IFTDGN", "IFTFCC", "IFTMAN", "IFTMBC", "IFTMBF", "IFTMBP", "IFTMCA",
            "IFTMCS", "IFTMDG", "IFTMED", "IFTMFS", "IFTMMB", "IFTMRC", "IFTMRD", "IFTMVG",
            "IFTSAI", "IFTSTA", "INSDES", "INSREQ", "INVOIC", "INVRPT", "IPPOAD", "IPPOMO",
            "ITRRPT", "JAPRES", "JINFDE", "JOBAPP", "JOBCON", "JOBMOD", "JOBOFF", "MEDPID",
            "MEDREQ", "MEDRPT", "MEQPOS", "MOVINS", "ORDCHG", "ORDERS", "ORDRSP", "OSTENQ",
            "OSTRPT", "PARTIN", "PASREQ", "PASRPT", "PAXLST", "PAYEXT", "PAYMUL", "PAYORD",
            "PRICAT", "PRODAT", "PRODEX", "PROINQ", "PROSRV", "QALITY", "QUOTES", "RDRMES",
            "REBORD", "RECADV", "RECALC", "RECECO", "RECLAM", "REMADV", "REQDOC", "REQOTE",
            "RESETT", "RESMSG", "RESRSP", "SANCRT", "SLSPRT", "SLSRPT", "SOCADE", "SSIMOD",
            "STATAC", "SUPCOT", "SUPMAN", "SUPRES", "TANSTA", "TAXCON", "TIQREQ", "TIQRPT",
            "VATDEC", "VESDEP", "WASDIS", "WKGRDC",
        ];

        let msg_type_upper = msg_type.message_type.to_uppercase();
        if !valid_types.contains(&msg_type_upper.as_str()) {
            // This is a warning, not an error - unknown message types might be valid
            // in specific trading partner agreements
            return Err(ValidationError::new(
                format!(
                    "Unknown message type: '{}' (may be valid for specific agreements)",
                    msg_type.message_type
                ),
                EnvelopeType::Message,
                Position::default(),
                ErrorKind::InvalidMessageType,
            ));
        }

        // Validate agency code
        let valid_agencies = [
            "UN", "EAN", "EDIFICE", "ODETTE", "VDA", "AIAG", "CEFIC", "ECR",
        ];
        if !valid_agencies.contains(&msg_type.agency.as_str()) {
            // This is a warning for unknown agencies
            return Err(ValidationError::new(
                format!(
                    "Unknown agency: '{}' (may be valid for specific agreements)",
                    msg_type.agency
                ),
                EnvelopeType::Message,
                Position::default(),
                ErrorKind::InvalidMessageType,
            ));
        }

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SegmentParser;
    use tempfile::TempDir;

    #[test]
    fn test_parse_unb() {
        let data = b"UNB+UNOA:3+SENDER+RECEIVER+200101:1200+12345'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unb = parse_unb(&segment).unwrap();
        assert_eq!(unb.syntax_identifier.identifier, "UNOA");
        assert_eq!(unb.syntax_identifier.version, "3");
        assert_eq!(unb.sender.id, "SENDER");
        assert_eq!(unb.receiver.id, "RECEIVER");
        assert_eq!(unb.datetime.date, "200101");
        assert_eq!(unb.datetime.time, "1200");
        assert_eq!(unb.control_ref, "12345");
    }

    #[test]
    fn test_parse_unb_with_qualifiers() {
        let data = b"UNB+UNOA:3+SENDER:14:INTERNAL:ZZ+RECEIVER:14:INTERNAL:ZZ+200101:1200+12345'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unb = parse_unb(&segment).unwrap();
        assert_eq!(unb.sender.id, "SENDER");
        assert_eq!(unb.sender.qualifier, Some("14".to_string()));
        assert_eq!(unb.sender.internal_id, Some("INTERNAL".to_string()));
        assert_eq!(unb.sender.internal_qualifier, Some("ZZ".to_string()));
    }

    #[test]
    fn test_parse_unb_with_optional_fields() {
        let data = b"UNB+UNOA:3+SENDER+RECEIVER+200101:1200+12345+APPREF+A+1+AGREEMENT+1'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unb = parse_unb(&segment).unwrap();
        assert_eq!(unb.application_ref, Some("APPREF".to_string()));
        assert_eq!(unb.priority, Some("A".to_string()));
        assert_eq!(unb.ack_request, Some("1".to_string()));
        assert_eq!(unb.comms_agreement_id, Some("AGREEMENT".to_string()));
        assert_eq!(unb.test_indicator, Some("1".to_string()));
    }

    #[test]
    fn test_parse_unz() {
        let data = b"UNZ+5+12345'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unz = parse_unz(&segment).unwrap();
        assert_eq!(unz.message_count, 5);
        assert_eq!(unz.control_ref, "12345");
    }

    #[test]
    fn test_parse_unh() {
        let data = b"UNH+1+ORDERS:D:96A:UN'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unh = parse_unh(&segment).unwrap();
        assert_eq!(unh.message_ref, "1");
        assert_eq!(unh.message_type.message_type, "ORDERS");
        assert_eq!(unh.message_type.version, "D");
        assert_eq!(unh.message_type.release, "96A");
        assert_eq!(unh.message_type.agency, "UN");
    }

    #[test]
    fn test_parse_unh_with_association_code() {
        let data = b"UNH+1+ORDERS:D:96A:UN:EAN123'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unh = parse_unh(&segment).unwrap();
        assert_eq!(
            unh.message_type.association_code,
            Some("EAN123".to_string())
        );
    }

    #[test]
    fn test_parse_unt() {
        let data = b"UNT+15+1'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let unt = parse_unt(&segment).unwrap();
        assert_eq!(unt.segment_count, 15);
        assert_eq!(unt.message_ref, "1");
    }

    #[test]
    fn test_parse_una() {
        let data = b"UNA:+.? '";
        let una = parse_una(data).unwrap();
        assert_eq!(una.component_separator, b':');
        assert_eq!(una.element_separator, b'+');
        assert_eq!(una.decimal_point, b'.');
        assert_eq!(una.release_character, b'?');
        assert_eq!(una.segment_terminator, b'\'');
    }

    #[test]
    fn test_parse_una_custom() {
        let data = b"UNA*=_# ~";
        let una = parse_una(data).unwrap();
        assert_eq!(una.component_separator, b'*');
        assert_eq!(una.element_separator, b'=');
        assert_eq!(una.decimal_point, b'_');
        assert_eq!(una.release_character, b'#');
        assert_eq!(una.segment_terminator, b'~');
    }

    #[test]
    fn test_parse_una_invalid_too_short() {
        let data = b"UNA:+.?";
        let result = parse_una(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_una_invalid_prefix() {
        let data = b"UNB:+.? '";
        let result = parse_una(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_unb() {
        let separators = crate::syntax::Separators::default();
        let unb = UnbSegment {
            syntax_identifier: SyntaxIdentifier::default(),
            sender: PartyId {
                id: "SENDER".to_string(),
                qualifier: None,
                internal_id: None,
                internal_qualifier: None,
            },
            receiver: PartyId {
                id: "RECEIVER".to_string(),
                qualifier: None,
                internal_id: None,
                internal_qualifier: None,
            },
            datetime: DateTime {
                date: "200101".to_string(),
                time: "1200".to_string(),
            },
            control_ref: "12345".to_string(),
            application_ref: None,
            priority: None,
            ack_request: None,
            comms_agreement_id: None,
            test_indicator: None,
        };

        let segment = generate_unb(&unb, &separators);
        assert_eq!(segment.tag, "UNB");
        assert_eq!(segment.elements.len(), 5);
    }

    #[test]
    fn test_generate_unb_with_optional() {
        let separators = crate::syntax::Separators::default();
        let unb = UnbSegment {
            syntax_identifier: SyntaxIdentifier::default(),
            sender: PartyId::default(),
            receiver: PartyId::default(),
            datetime: DateTime::default(),
            control_ref: "12345".to_string(),
            application_ref: Some("APP".to_string()),
            priority: None,
            ack_request: None,
            comms_agreement_id: None,
            test_indicator: None,
        };

        let segment = generate_unb(&unb, &separators);
        assert_eq!(segment.elements.len(), 6);
    }

    #[test]
    fn test_generate_unz() {
        let separators = crate::syntax::Separators::default();
        let unz = UnzSegment {
            message_count: 3,
            control_ref: "12345".to_string(),
        };

        let segment = generate_unz(&unz, &separators);
        assert_eq!(segment.tag, "UNZ");
        assert_eq!(segment.elements.len(), 2);
    }

    #[test]
    fn test_generate_unh() {
        let separators = crate::syntax::Separators::default();
        let unh = UnhSegment {
            message_ref: "1".to_string(),
            message_type: MessageTypeIdentifier::default(),
            common_access_ref: None,
            transfer_status: None,
            subset_id: None,
            implementation_id: None,
            scenario_id: None,
        };

        let segment = generate_unh(&unh, &separators);
        assert_eq!(segment.tag, "UNH");
        assert_eq!(segment.elements.len(), 2);
    }

    #[test]
    fn test_generate_unt() {
        let separators = crate::syntax::Separators::default();
        let unt = UntSegment {
            segment_count: 10,
            message_ref: "1".to_string(),
        };

        let segment = generate_unt(&unt, &separators);
        assert_eq!(segment.tag, "UNT");
        assert_eq!(segment.elements.len(), 2);
    }

    #[test]
    fn test_validate_matching_refs() {
        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 1,
                control_ref: "12345".to_string(),
            }),
            messages: vec![MessageEnvelope {
                unh: UnhSegment {
                    message_ref: "1".to_string(),
                    message_type: MessageTypeIdentifier::default(),
                    common_access_ref: None,
                    transfer_status: None,
                    subset_id: None,
                    implementation_id: None,
                    scenario_id: None,
                },
                unt: Some(UntSegment {
                    segment_count: 5,
                    message_ref: "1".to_string(),
                }),
                segments: vec![
                    Segment {
                        tag: "BGM".to_string(),
                        elements: vec![],
                        position: Position::default(),
                    };
                    3
                ],
            }],
        };

        assert!(validate_interchange(&interchange).is_ok());
    }

    #[test]
    fn test_validate_mismatched_interchange_refs() {
        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 1,
                control_ref: "54321".to_string(),
            }),
            messages: vec![],
        };

        let result = validate_interchange(&interchange);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("control reference mismatch"));
    }

    #[test]
    fn test_validate_mismatched_message_refs() {
        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: Some(UntSegment {
                segment_count: 2,
                message_ref: "2".to_string(),
            }),
            segments: vec![],
        };

        let result = validate_message(&message);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("reference mismatch"));
    }

    #[test]
    fn test_validate_message_count_mismatch() {
        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 5,
                control_ref: "12345".to_string(),
            }),
            messages: vec![MessageEnvelope {
                unh: UnhSegment {
                    message_ref: "1".to_string(),
                    message_type: MessageTypeIdentifier::default(),
                    common_access_ref: None,
                    transfer_status: None,
                    subset_id: None,
                    implementation_id: None,
                    scenario_id: None,
                },
                unt: Some(UntSegment {
                    segment_count: 2,
                    message_ref: "1".to_string(),
                }),
                segments: vec![],
            }],
        };

        let result = validate_interchange(&interchange);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Message count mismatch"));
    }

    #[test]
    fn test_validate_segment_count_mismatch() {
        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: Some(UntSegment {
                segment_count: 10,
                message_ref: "1".to_string(),
            }),
            segments: vec![
                Segment {
                    tag: "BGM".to_string(),
                    elements: vec![],
                    position: Position::default(),
                };
                3
            ],
        };

        let result = validate_message(&message);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Segment count mismatch"));
    }

    #[test]
    fn test_validate_missing_unz() {
        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: None,
            messages: vec![],
        };

        let result = validate_interchange(&interchange);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Missing UNZ"));
    }

    #[test]
    fn test_validate_missing_unt() {
        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: None,
            segments: vec![],
        };

        let result = validate_message(&message);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Missing UNT"));
    }

    #[test]
    fn test_memory_control_number_generator() {
        let gen = MemoryControlNumberGenerator::new();

        // Test interchange refs
        let ref1 = gen.next_interchange_ref().unwrap();
        let ref2 = gen.next_interchange_ref().unwrap();
        assert_eq!(ref1, "00000000000001");
        assert_eq!(ref2, "00000000000002");

        // Test message refs
        let msg1 = gen.next_message_ref(&ref1).unwrap();
        let msg2 = gen.next_message_ref(&ref1).unwrap();
        let msg3 = gen.next_message_ref("other").unwrap();

        assert_eq!(msg1, "000001");
        assert_eq!(msg2, "000002");
        assert_eq!(msg3, "000001");

        // Reset and verify
        gen.reset().unwrap();
        let ref3 = gen.next_interchange_ref().unwrap();
        assert_eq!(ref3, "00000000000001");
    }

    #[test]
    fn test_file_based_control_number_generator() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("control_numbers.json");
        let file_path_str = file_path.to_str().unwrap();

        let gen = FileBasedControlNumberGenerator::new(file_path_str).unwrap();

        // Generate some numbers
        let ref1 = gen.next_interchange_ref().unwrap();
        assert_eq!(ref1, "00000000000001");

        let msg1 = gen.next_message_ref(&ref1).unwrap();
        assert_eq!(msg1, "000001");

        // Create new generator from same file
        drop(gen);
        let gen2 = FileBasedControlNumberGenerator::new(file_path_str).unwrap();

        // Should continue from where we left off
        let ref2 = gen2.next_interchange_ref().unwrap();
        assert_eq!(ref2, "00000000000002");

        let msg2 = gen2.next_message_ref(&ref1).unwrap();
        assert_eq!(msg2, "000002");

        // Reset
        gen2.reset().unwrap();
        let ref3 = gen2.next_interchange_ref().unwrap();
        assert_eq!(ref3, "00000000000001");
    }

    #[test]
    fn test_multi_message_interchange() {
        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 3,
                control_ref: "12345".to_string(),
            }),
            messages: vec![
                MessageEnvelope {
                    unh: UnhSegment {
                        message_ref: "1".to_string(),
                        message_type: MessageTypeIdentifier::default(),
                        common_access_ref: None,
                        transfer_status: None,
                        subset_id: None,
                        implementation_id: None,
                        scenario_id: None,
                    },
                    unt: Some(UntSegment {
                        segment_count: 5,
                        message_ref: "1".to_string(),
                    }),
                    segments: vec![
                        Segment {
                            tag: "BGM".to_string(),
                            elements: vec![],
                            position: Position::default(),
                        };
                        3
                    ],
                },
                MessageEnvelope {
                    unh: UnhSegment {
                        message_ref: "2".to_string(),
                        message_type: MessageTypeIdentifier {
                            message_type: "DESADV".to_string(),
                            ..Default::default()
                        },
                        common_access_ref: None,
                        transfer_status: None,
                        subset_id: None,
                        implementation_id: None,
                        scenario_id: None,
                    },
                    unt: Some(UntSegment {
                        segment_count: 4,
                        message_ref: "2".to_string(),
                    }),
                    segments: vec![
                        Segment {
                            tag: "BGM".to_string(),
                            elements: vec![],
                            position: Position::default(),
                        };
                        2
                    ],
                },
                MessageEnvelope {
                    unh: UnhSegment {
                        message_ref: "3".to_string(),
                        message_type: MessageTypeIdentifier {
                            message_type: "INVOIC".to_string(),
                            ..Default::default()
                        },
                        common_access_ref: None,
                        transfer_status: None,
                        subset_id: None,
                        implementation_id: None,
                        scenario_id: None,
                    },
                    unt: Some(UntSegment {
                        segment_count: 6,
                        message_ref: "3".to_string(),
                    }),
                    segments: vec![
                        Segment {
                            tag: "BGM".to_string(),
                            elements: vec![],
                            position: Position::default(),
                        };
                        4
                    ],
                },
            ],
        };

        assert!(validate_interchange(&interchange).is_ok());
        assert_eq!(interchange.messages.len(), 3);
        assert_eq!(interchange.messages[0].unh.message_ref, "1");
        assert_eq!(interchange.messages[1].unh.message_ref, "2");
        assert_eq!(interchange.messages[2].unh.message_ref, "3");
    }

    #[test]
    fn test_una_to_separators() {
        let una = UnaSegment::default();
        let sep = una.to_separators();
        assert_eq!(sep.component, b':');
        assert_eq!(sep.element, b'+');
        assert_eq!(sep.decimal, b'.');
        assert_eq!(sep.release, b'?');
        assert_eq!(sep.segment, b'\'');
    }

    #[test]
    fn test_una_to_bytes() {
        let una = UnaSegment::default();
        let bytes = una.to_bytes();
        assert_eq!(bytes, b"UNA:+.? '");
    }

    #[test]
    fn test_una_custom_separators() {
        let una = UnaSegment {
            component_separator: b'*',
            element_separator: b'=',
            decimal_point: b'_',
            release_character: b'#',
            reserved: b' ',
            segment_terminator: b'~',
        };
        let bytes = una.to_bytes();
        assert_eq!(bytes, b"UNA*=_# ~");
    }

    #[test]
    fn test_una_from_separators() {
        let sep = crate::syntax::Separators {
            component: b'*',
            element: b'=',
            decimal: b'_',
            release: b'#',
            segment: b'~',
        };
        let una = UnaSegment::from_separators(sep);
        assert_eq!(una.component_separator, b'*');
        assert_eq!(una.element_separator, b'=');
        assert_eq!(una.decimal_point, b'_');
        assert_eq!(una.release_character, b'#');
        assert_eq!(una.segment_terminator, b'~');
    }

    #[test]
    fn test_parse_unb_error_wrong_tag() {
        let data = b"UNH+1+ORDERS:D:96A:UN'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let result = parse_unb(&segment);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected UNB"));
    }

    #[test]
    fn test_parse_unb_error_too_few_elements() {
        let data = b"UNB+UNOA:3+SENDER'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let result = parse_unb(&segment);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least 5 elements"));
    }

    #[test]
    fn test_parse_unz_error_wrong_tag() {
        let data = b"UNB+UNOA:3+SENDER+RECEIVER+200101:1200+12345'";
        let mut parser = SegmentParser::new(data, "test");
        let segment = parser.next_segment().unwrap().unwrap();

        let result = parse_unz(&segment);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected UNZ"));
    }

    #[test]
    fn test_generate_and_roundtrip() {
        let separators = crate::syntax::Separators::default();
        let unb = UnbSegment {
            syntax_identifier: SyntaxIdentifier {
                identifier: "UNOA".to_string(),
                version: "3".to_string(),
                service_code_list: None,
                encoding: None,
            },
            sender: PartyId {
                id: "SENDER".to_string(),
                qualifier: Some("14".to_string()),
                internal_id: None,
                internal_qualifier: None,
            },
            receiver: PartyId {
                id: "RECEIVER".to_string(),
                qualifier: Some("14".to_string()),
                internal_id: None,
                internal_qualifier: None,
            },
            datetime: DateTime {
                date: "200101".to_string(),
                time: "1200".to_string(),
            },
            control_ref: "00000000000001".to_string(),
            application_ref: None,
            priority: None,
            ack_request: None,
            comms_agreement_id: None,
            test_indicator: None,
        };

        // Generate segment
        let segment = generate_unb(&unb, &separators);

        // Parse it back
        let parsed = parse_unb(&segment).unwrap();
        assert_eq!(parsed.control_ref, unb.control_ref);
        assert_eq!(parsed.sender.id, unb.sender.id);
        assert_eq!(parsed.receiver.id, unb.receiver.id);
    }

    // ============================================================================
    // EnvelopeValidator Tests
    // ============================================================================

    #[test]
    fn test_valid_unb_unz_pairing() {
        let validator = EnvelopeValidator::new();

        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 1,
                control_ref: "12345".to_string(),
            }),
            messages: vec![MessageEnvelope {
                unh: UnhSegment {
                    message_ref: "1".to_string(),
                    message_type: MessageTypeIdentifier::default(),
                    common_access_ref: None,
                    transfer_status: None,
                    subset_id: None,
                    implementation_id: None,
                    scenario_id: None,
                },
                unt: Some(UntSegment {
                    segment_count: 5,
                    message_ref: "1".to_string(),
                }),
                segments: vec![
                    Segment {
                        tag: "BGM".to_string(),
                        elements: vec![],
                        position: Position::default(),
                    };
                    3
                ],
            }],
        };

        assert!(validator.validate_interchange(&interchange).is_ok());
    }

    #[test]
    fn test_mismatched_control_reference() {
        let validator = EnvelopeValidator::new();

        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 1,
                control_ref: "54321".to_string(),
            }),
            messages: vec![],
        };

        let result = validator.validate_interchange(&interchange);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::MismatchedControlReference);
        assert_eq!(error.expected, Some("12345".to_string()));
        assert_eq!(error.actual, Some("54321".to_string()));
    }

    #[test]
    fn test_missing_unz() {
        let validator = EnvelopeValidator::new();

        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: None,
            messages: vec![],
        };

        let result = validator.validate_interchange(&interchange);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::MissingTrailer);
    }

    #[test]
    fn test_incorrect_message_count() {
        let validator = EnvelopeValidator::new();

        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier::default(),
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 5,
                control_ref: "12345".to_string(),
            }),
            messages: vec![MessageEnvelope {
                unh: UnhSegment {
                    message_ref: "1".to_string(),
                    message_type: MessageTypeIdentifier::default(),
                    common_access_ref: None,
                    transfer_status: None,
                    subset_id: None,
                    implementation_id: None,
                    scenario_id: None,
                },
                unt: Some(UntSegment {
                    segment_count: 2,
                    message_ref: "1".to_string(),
                }),
                segments: vec![],
            }],
        };

        let result = validator.validate_interchange(&interchange);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::IncorrectMessageCount);
        assert_eq!(error.expected, Some("5".to_string()));
        assert_eq!(error.actual, Some("1".to_string()));
    }

    #[test]
    fn test_valid_unh_unt_pairing() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "MSG001".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: Some(UntSegment {
                segment_count: 5,
                message_ref: "MSG001".to_string(),
            }),
            segments: vec![
                Segment {
                    tag: "BGM".to_string(),
                    elements: vec![],
                    position: Position::default(),
                };
                3
            ],
        };

        assert!(validator.validate_message(&message).is_ok());
    }

    #[test]
    fn test_mismatched_message_reference() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "MSG001".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: Some(UntSegment {
                segment_count: 2,
                message_ref: "MSG002".to_string(),
            }),
            segments: vec![],
        };

        let result = validator.validate_message(&message);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::MismatchedControlReference);
        assert_eq!(error.expected, Some("MSG001".to_string()));
        assert_eq!(error.actual, Some("MSG002".to_string()));
    }

    #[test]
    fn test_incorrect_segment_count() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: Some(UntSegment {
                segment_count: 10,
                message_ref: "1".to_string(),
            }),
            segments: vec![
                Segment {
                    tag: "BGM".to_string(),
                    elements: vec![],
                    position: Position::default(),
                };
                3
            ],
        };

        let result = validator.validate_message(&message);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::IncorrectSegmentCount);
        // Expected 10, actual is 5 (3 segments + UNH + UNT)
        assert_eq!(error.expected, Some("10".to_string()));
        assert_eq!(error.actual, Some("5".to_string()));
    }

    #[test]
    fn test_missing_unt() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: None,
            segments: vec![],
        };

        let result = validator.validate_message(&message);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.error_kind, ErrorKind::MissingTrailer);
    }

    #[test]
    fn test_full_document_validation() {
        let validator = EnvelopeValidator::new();

        let document = EdifactDocument {
            una: None,
            interchange: Some(InterchangeEnvelope {
                unb: UnbSegment {
                    syntax_identifier: SyntaxIdentifier::default(),
                    sender: PartyId::default(),
                    receiver: PartyId::default(),
                    datetime: DateTime::default(),
                    control_ref: "INTERCHANGE001".to_string(),
                    application_ref: None,
                    priority: None,
                    ack_request: None,
                    comms_agreement_id: None,
                    test_indicator: None,
                },
                unz: Some(UnzSegment {
                    message_count: 2,
                    control_ref: "INTERCHANGE001".to_string(),
                }),
                messages: vec![
                    MessageEnvelope {
                        unh: UnhSegment {
                            message_ref: "1".to_string(),
                            message_type: MessageTypeIdentifier::default(),
                            common_access_ref: None,
                            transfer_status: None,
                            subset_id: None,
                            implementation_id: None,
                            scenario_id: None,
                        },
                        unt: Some(UntSegment {
                            segment_count: 5,
                            message_ref: "1".to_string(),
                        }),
                        segments: vec![
                            Segment {
                                tag: "BGM".to_string(),
                                elements: vec![],
                                position: Position::default(),
                            };
                            3
                        ],
                    },
                    MessageEnvelope {
                        unh: UnhSegment {
                            message_ref: "2".to_string(),
                            message_type: MessageTypeIdentifier {
                                message_type: "DESADV".to_string(),
                                ..Default::default()
                            },
                            common_access_ref: None,
                            transfer_status: None,
                            subset_id: None,
                            implementation_id: None,
                            scenario_id: None,
                        },
                        unt: Some(UntSegment {
                            segment_count: 4,
                            message_ref: "2".to_string(),
                        }),
                        segments: vec![
                            Segment {
                                tag: "BGM".to_string(),
                                elements: vec![],
                                position: Position::default(),
                            };
                            2
                        ],
                    },
                ],
            }),
        };

        let report = validator.validate(&document).unwrap();
        assert!(report.is_valid);
        assert_eq!(report.error_count(), 0);
        assert_eq!(report.warning_count(), 0);
    }

    #[test]
    fn test_document_validation_with_errors() {
        let validator = EnvelopeValidator::new();

        let document = EdifactDocument {
            una: None,
            interchange: Some(InterchangeEnvelope {
                unb: UnbSegment {
                    syntax_identifier: SyntaxIdentifier::default(),
                    sender: PartyId::default(),
                    receiver: PartyId::default(),
                    datetime: DateTime::default(),
                    control_ref: "INTERCHANGE001".to_string(),
                    application_ref: None,
                    priority: None,
                    ack_request: None,
                    comms_agreement_id: None,
                    test_indicator: None,
                },
                unz: Some(UnzSegment {
                    message_count: 1,
                    control_ref: "WRONG_REF".to_string(), // Mismatched!
                }),
                messages: vec![MessageEnvelope {
                    unh: UnhSegment {
                        message_ref: "1".to_string(),
                        message_type: MessageTypeIdentifier::default(),
                        common_access_ref: None,
                        transfer_status: None,
                        subset_id: None,
                        implementation_id: None,
                        scenario_id: None,
                    },
                    unt: Some(UntSegment {
                        segment_count: 100, // Wrong count
                        message_ref: "1".to_string(),
                    }),
                    segments: vec![
                        Segment {
                            tag: "BGM".to_string(),
                            elements: vec![],
                            position: Position::default(),
                        };
                        3
                    ],
                }],
            }),
        };

        let report = validator.validate(&document).unwrap();
        assert!(!report.is_valid);
        assert_eq!(report.error_count(), 2); // Interchange ref mismatch + segment count mismatch
    }

    #[test]
    fn test_document_without_interchange() {
        let validator = EnvelopeValidator::new();

        let document = EdifactDocument {
            una: None,
            interchange: None,
        };

        let report = validator.validate(&document).unwrap();
        assert!(!report.is_valid);
        assert_eq!(report.error_count(), 1);
        assert_eq!(report.errors[0].error_kind, ErrorKind::MissingHeader);
    }

    #[test]
    fn test_check_control_reference_unb_unz() {
        let validator = EnvelopeValidator::new();

        let unb = Segment {
            tag: "UNB".to_string(),
            elements: vec![
                Element::Composite(vec![b"UNOA".to_vec(), b"3".to_vec()]),
                Element::Composite(vec![b"SENDER".to_vec()]),
                Element::Composite(vec![b"RECEIVER".to_vec()]),
                Element::Composite(vec![b"200101".to_vec(), b"1200".to_vec()]),
                Element::Simple(b"REF123".to_vec()),
            ],
            position: Position::default(),
        };

        let unz = Segment {
            tag: "UNZ".to_string(),
            elements: vec![
                Element::Simple(b"1".to_vec()),
                Element::Simple(b"REF123".to_vec()),
            ],
            position: Position::default(),
        };

        assert!(validator.check_control_reference(&unb, &unz).is_ok());
    }

    #[test]
    fn test_check_control_reference_mismatch() {
        let validator = EnvelopeValidator::new();

        let unb = Segment {
            tag: "UNB".to_string(),
            elements: vec![
                Element::Composite(vec![b"UNOA".to_vec(), b"3".to_vec()]),
                Element::Composite(vec![b"SENDER".to_vec()]),
                Element::Composite(vec![b"RECEIVER".to_vec()]),
                Element::Composite(vec![b"200101".to_vec(), b"1200".to_vec()]),
                Element::Simple(b"REF123".to_vec()),
            ],
            position: Position::default(),
        };

        let unz = Segment {
            tag: "UNZ".to_string(),
            elements: vec![
                Element::Simple(b"1".to_vec()),
                Element::Simple(b"WRONG".to_vec()),
            ],
            position: Position::default(),
        };

        let result = validator.check_control_reference(&unb, &unz);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mismatch"));
    }

    #[test]
    fn test_check_control_reference_unh_unt() {
        let validator = EnvelopeValidator::new();

        let unh = Segment {
            tag: "UNH".to_string(),
            elements: vec![
                Element::Simple(b"MSG001".to_vec()),
                Element::Composite(vec![
                    b"ORDERS".to_vec(),
                    b"D".to_vec(),
                    b"96A".to_vec(),
                    b"UN".to_vec(),
                ]),
            ],
            position: Position::default(),
        };

        let unt = Segment {
            tag: "UNT".to_string(),
            elements: vec![
                Element::Simple(b"5".to_vec()),
                Element::Simple(b"MSG001".to_vec()),
            ],
            position: Position::default(),
        };

        assert!(validator.check_control_reference(&unh, &unt).is_ok());
    }

    #[test]
    fn test_check_segment_count() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: None,
            segments: vec![
                Segment {
                    tag: "BGM".to_string(),
                    elements: vec![],
                    position: Position::default(),
                };
                3
            ],
        };

        // 3 segments + UNH + UNT = 5
        let unt_segment = Segment {
            tag: "UNT".to_string(),
            elements: vec![
                Element::Simple(b"5".to_vec()),
                Element::Simple(b"1".to_vec()),
            ],
            position: Position::default(),
        };

        assert!(validator
            .check_segment_count(&message, &unt_segment)
            .is_ok());
    }

    #[test]
    fn test_check_segment_count_mismatch() {
        let validator = EnvelopeValidator::new();

        let message = MessageEnvelope {
            unh: UnhSegment {
                message_ref: "1".to_string(),
                message_type: MessageTypeIdentifier::default(),
                common_access_ref: None,
                transfer_status: None,
                subset_id: None,
                implementation_id: None,
                scenario_id: None,
            },
            unt: None,
            segments: vec![
                Segment {
                    tag: "BGM".to_string(),
                    elements: vec![],
                    position: Position::default(),
                };
                3
            ],
        };

        // Wrong count: should be 5 but we say 10
        let unt_segment = Segment {
            tag: "UNT".to_string(),
            elements: vec![
                Element::Simple(b"10".to_vec()),
                Element::Simple(b"1".to_vec()),
            ],
            position: Position::default(),
        };

        let result = validator.check_segment_count(&message, &unt_segment);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mismatch"));
    }

    #[test]
    fn test_invalid_syntax_identifier() {
        let validator = EnvelopeValidator::new();

        let interchange = InterchangeEnvelope {
            unb: UnbSegment {
                syntax_identifier: SyntaxIdentifier {
                    identifier: "INVALID".to_string(),
                    version: "3".to_string(),
                    service_code_list: None,
                    encoding: None,
                },
                sender: PartyId::default(),
                receiver: PartyId::default(),
                datetime: DateTime::default(),
                control_ref: "12345".to_string(),
                application_ref: None,
                priority: None,
                ack_request: None,
                comms_agreement_id: None,
                test_indicator: None,
            },
            unz: Some(UnzSegment {
                message_count: 0,
                control_ref: "12345".to_string(),
            }),
            messages: vec![],
        };

        let result = validator.validate_interchange(&interchange);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().error_kind,
            ErrorKind::InvalidSyntaxIdentifier
        );
    }

    #[test]
    fn test_validation_report_merge() {
        let mut report1 = ValidationReport::new();
        report1.add_error(ValidationError::new(
            "Error 1",
            EnvelopeType::Interchange,
            Position::default(),
            ErrorKind::MissingHeader,
        ));
        report1.add_warning(ValidationWarning::new(
            "Warning 1",
            EnvelopeType::Message,
            Position::default(),
        ));

        let mut report2 = ValidationReport::new();
        report2.add_error(ValidationError::new(
            "Error 2",
            EnvelopeType::Message,
            Position::default(),
            ErrorKind::MissingTrailer,
        ));

        report1.merge(report2);

        assert!(!report1.is_valid);
        assert_eq!(report1.error_count(), 2);
        assert_eq!(report1.warning_count(), 1);
    }

    #[test]
    fn test_edifact_document_helpers() {
        let doc = EdifactDocument::new();
        assert!(!doc.has_interchange());
        assert_eq!(doc.message_count(), 0);

        let doc_with_interchange = EdifactDocument {
            una: None,
            interchange: Some(InterchangeEnvelope {
                unb: UnbSegment {
                    syntax_identifier: SyntaxIdentifier::default(),
                    sender: PartyId::default(),
                    receiver: PartyId::default(),
                    datetime: DateTime::default(),
                    control_ref: "12345".to_string(),
                    application_ref: None,
                    priority: None,
                    ack_request: None,
                    comms_agreement_id: None,
                    test_indicator: None,
                },
                unz: Some(UnzSegment {
                    message_count: 2,
                    control_ref: "12345".to_string(),
                }),
                messages: vec![
                    MessageEnvelope {
                        unh: UnhSegment {
                            message_ref: "1".to_string(),
                            message_type: MessageTypeIdentifier::default(),
                            common_access_ref: None,
                            transfer_status: None,
                            subset_id: None,
                            implementation_id: None,
                            scenario_id: None,
                        },
                        unt: Some(UntSegment {
                            segment_count: 2,
                            message_ref: "1".to_string(),
                        }),
                        segments: vec![],
                    },
                    MessageEnvelope {
                        unh: UnhSegment {
                            message_ref: "2".to_string(),
                            message_type: MessageTypeIdentifier::default(),
                            common_access_ref: None,
                            transfer_status: None,
                            subset_id: None,
                            implementation_id: None,
                            scenario_id: None,
                        },
                        unt: Some(UntSegment {
                            segment_count: 2,
                            message_ref: "2".to_string(),
                        }),
                        segments: vec![],
                    },
                ],
            }),
        };

        assert!(doc_with_interchange.has_interchange());
        assert_eq!(doc_with_interchange.message_count(), 2);
    }
}
