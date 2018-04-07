import java.util.HashMap;

public class Entity_v2 {
	Integer docId;
	Long docLength;
	Long parent_docLength;
	Integer parent_docId;
	private HashMap<String,Long> term_counts;
	private HashMap<String,Long> parent_term_counts;

	String passage_text;
	

	public String getPassage_text() {
		return passage_text;
	}

	public void setPassage_text(String passage_text) {
		this.passage_text = passage_text;
	}

	public Long getParent_docLength() {
		return parent_docLength;
	}

	public void setParent_docLength(Long parent_docLength) {
		this.parent_docLength = parent_docLength;
	}

	public Integer getParent_docId() {
		return parent_docId;
	}

	public void setParent_docId(Integer parent_docId) {
		this.parent_docId = parent_docId;
	}

	public HashMap<String, Long> getParent_term_counts() {
		return parent_term_counts;
	}

	public void setParent_term_counts(HashMap<String, Long> parent_term_counts) {
		this.parent_term_counts = parent_term_counts;
	}

	public Long getDocLength() {
		return docLength;
	}

	public void setDocLength(Long docLength) {
		this.docLength = docLength;
	}

	public Integer getDocId() {
		return docId;
	}

	public void setDocId(Integer docId) {
		this.docId = docId;
	}

	public HashMap<String, Long> getTermCounts() {
		return term_counts;
	}

	public void setTermCounts(HashMap<String, Long> docs) {
		this.term_counts = docs;
	}
	
}
