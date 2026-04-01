package com.amazon.kinesis.kafka;

public class KmsRequestDto {

	private int projectId;
	private int systemSeq;
	private String password;
	private String username;
	private int connectionSequence;
	private String connectionName;
	private String connType;
	private String sourceTargetType;
	private String juniperUser;
	private String purposeId;
	private String operationType;
	private int credentialId;

	public int getCredentialId() {
		return credentialId;
	}

	public void setCredentialId(int credentialId) {
		this.credentialId = credentialId;
	}

	public String getPurposeId() {
		return purposeId;
	}

	public void setPurposeId(String puposeId) {
		this.purposeId = puposeId;
	}

	public int getProjectId() {
		return projectId;
	}

	public void setProjectId(int projectId) {
		this.projectId = projectId;
	}

	public int getSystemSeq() {
		return systemSeq;
	}

	public void setSystemSeq(int systemSeq) {
		this.systemSeq = systemSeq;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getOperationType() {
		return operationType;
	}

	public void setOperationType(String operationType) {
		this.operationType = operationType;
	}

	public int getConnectionSequence() {
		return connectionSequence;
	}

	public void setConnectionSequence(int connectionSequence) {
		this.connectionSequence = connectionSequence;
	}

	public String getConnectionName() {
		return connectionName;
	}

	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

	public String getConnType() {
		return connType;
	}

	public void setConnType(String connType) {
		this.connType = connType;
	}

	public String getSourceTargetType() {
		return sourceTargetType;
	}

	public void setSourceTargetType(String sourceTargetType) {
		this.sourceTargetType = sourceTargetType;
	}

	public String getJuniperUser() {
		return juniperUser;
	}

	public void setJuniperUser(String juniperUser) {
		this.juniperUser = juniperUser;
	}
}
