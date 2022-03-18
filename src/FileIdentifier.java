public class FileIdentifier {
	// declaring id and file_name
	private int fileId;
	private String file;

	// declaring constructors
	public FileIdentifier(int fileId, String file) {
		super();
		this.fileId = fileId;
		this.file = file;
	}

	// declaring getters and setters
	public FileIdentifier(String file) {
		this.file = file;
	}

	public int getFileId() {
		return fileId;
	}


	public void setFileId(int fileId) {
		this.fileId = fileId;
	}


	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}
}
