USE [HyperloopCluster]
GO

/****** Object:  Table [dbo].[Clusters]    Script Date: 2017-02-01 13:39:37 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Clusters](
	[ClusterID] [bigint] NULL,
	[ClusterName] [varchar](100) NULL,
	[ClusterObs] [varchar](250) NULL,
	[CentroidNo] [bigint] NULL,
	[CustomerNo] [bigint] NULL,
	[ClusterDate] [bigint] NOT NULL,
	[F1Obs] [varchar](100) NULL,
	[F2Obs] [varchar](100) NULL,
	[F3Obs] [varchar](100) NULL,
	[F4Obs] [varchar](100) NULL,
	[F5Obs] [varchar](100) NULL,
	[ClusterTypeID] [bigint] NULL,
	[ClusterTypeName] [varchar](100) NULL
) ON [PRIMARY]

GO

