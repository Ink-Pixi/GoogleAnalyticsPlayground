USE EmailDashboard
GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.tblRecentEmailSendingSummary') AND type in (N'U'))
DROP TABLE dbo.tblRecentEmailSendingSummary
GO

CREATE TABLE dbo.tblRecentEmailSendingSummary
(
	Id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
	JobID INT UNIQUE,
	Name VARCHAR(150),
	Subject VARCHAR(255),
	SendStartTime DATETIME,
	SendCompleteTime DATETIME,
	EmailsSent INT,
	EmailsDelivered INT,
	Undeliverable INT,
	SurveyResponses INT,
	TotalClickThroughs INT,
	UniqueClickThroughs INT,
	UniqueEmailsOpened INT,
	Unsubscribes INT,
	FTAFForwarders INT,
	FTAFRecipients INT,
	FTAFSubscribers INT,
	OpenRate DECIMAL(5,2),
	DeliverabilityRate DECIMAL(5,2),
	BounceRate DECIMAL(5,2),
	UnsubscribeRate DECIMAL(5,2),
	UniqueClickThroughRate DECIMAL(5,2),
	UniqueComplaints INT,
	CumulativeComplaints INT 
)