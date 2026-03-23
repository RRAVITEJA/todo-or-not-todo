CREATE TABLE reports (
    report_id        BIGINT PRIMARY KEY,
    name             VARCHAR(255) NOT NULL,
    description      VARCHAR(1000),
    created_at       TIMESTAMP DEFAULT GETDATE(),
    updated_at       TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO;

CREATE TABLE report_sections (
    report_section_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    report_id         BIGINT NOT NULL,
    section_id        INTEGER NOT NULL,
    
    created_at        TIMESTAMP DEFAULT GETDATE(),

    FOREIGN KEY (report_id) REFERENCES reports(report_id)
)
DISTKEY(report_id)
SORTKEY(report_id, section_id);

CREATE TABLE report_section_attributes (
    id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    report_id    BIGINT NOT NULL,
    section_id   INTEGER NOT NULL,
    attribute_id INTEGER NOT NULL,

    created_at   TIMESTAMP DEFAULT GETDATE()
)
DISTKEY(report_id)
SORTKEY(report_id, section_id, attribute_id);

ALTER TABLE reports ADD org_id VARCHAR;
ALTER TABLE reports ADD last_run_date TIMESTAMP;
ALTER TABLE report_sections ADD org_id VARCHAR;
ALTER TABLE report_section_attributes ADD org_id VARCHAR;