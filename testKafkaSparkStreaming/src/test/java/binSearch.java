public class binSearch{
	public static void main(String[] args) {
		int[] a = {12,20,5,16,15,1,30,45,23,8};
		int i = binSearchf(a, 23);
		System.out.println(i);
	}
	//非递归
	public static int binSearchf(int[] array,int key){
		int start=0;
		int mid;
		int end=array.length-1;
		while(start<=end){
			mid=(end-start)/2+start;
			if(key<array[mid]){
				end=mid-1;
			}
			else if(key>array[mid]){
				start=mid+1;
			}else{
				return mid;
			}
		}
		return -1;
	}
	//递归
	public static int binSearch1(int[] array,int key,int start,int end){
		int mid=(end-start)/2+start;
		if(key==array[mid]){
			return mid;
		}
		else if(start>=end){
			return -1;
		}
		else if(key>array[mid]){
			return binSearch1(array,key,mid+1,end);
		}
		else if(key<array[mid]){
			return binSearch1(array,key,start,mid-1);
		}
		return -1;
	}

}

