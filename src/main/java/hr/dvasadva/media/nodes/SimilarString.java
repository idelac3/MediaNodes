package hr.dvasadva.media.nodes;

class SimilarString {

	public static boolean compare(final String str1, final String str2) {
		
		boolean result = false;
		
		if (str1 == null && str2 == null) {
			
			return true;
		}
		
		if (str1 == null || str2 == null) {
			
			return false;
		}
		
		if (str1.length() == 0 && str2.length() == 0) {
			
			return true;
		}
		
		if (str1.length() == 0 || str2.length() == 0) {
			
			return false;
		}

		String[] words1 = str1.split("\\s+");
		String[] words2 = str2.split("\\s+");
		
		int match = 0, total = 0;
		for (final String word1 : words1) {
		
			if (contains(word1, words2)) {
				
				match++;
			}
			
			total++;
		}
		
		double ration = 1.00f * match / total;
		
		result = (ration >= 0.50);
		
		return result;
	}
	
	private static boolean contains(final String word, final String[] words) {
		
		for (final String w : words) {
			
			if (w != null && w.equalsIgnoreCase(word)) {
				
				return true;
			}
		}
		
		return false;
	}
}
