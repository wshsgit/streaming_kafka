public class binSearch{
	public static void main(String[] args) {
		int[] a = {1,5,8,12,15,16,20,23,30,45};
//		int i = binSearchf(a, 23);
		int i = searchDGTest(a,0,9,47);
		System.out.println(i);
	}
	//非递归
	public static int binSearchf(int[] array,int key){
		int start=0;
		int mid;
		int end=array.length-1;
		while(start<=end){//二分查找，当这个数组长度大于一
			mid=(end-start)/2+start;//找出中间值
			if(key<array[mid]){//如果要找的这个值小于这个数组的中间值
				end=mid-1;//将数组的最大值调成中间值，也就是说中间值的前面一部分
			}
			else if(key>array[mid]){//如果要找的这个值大于这个数组中间值
				start=mid+1;//那么将这个数组的最小值变成中间值，也就是中间值后面一部分
			}else{
				return mid;//最后的一种情况就是正好和这个数组的中间值相等
			}
		}
		return -1;//如果以上三种情况都没有，那么返回-1

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




	public static int searchTest(int[] a,int key){
		int start =0;
		int end = a.length-1;
		int mid ;
		while(start<end){
			mid = (start+end)/2;
			if(key<a[mid]){
				end=mid-1;
			}else if (key>a[mid]){
				start=mid+1;
			}else{
				return mid;
			}
		}
		return -1;

	}




	public static int searchDGTest(int[] a,int start,int end,int key){

		int mid = (start+end)/2;
		if(key==a[mid]){
			return mid;
		}
		else if(start>end){
			return -1;
		}else if(key>a[mid]){
			return searchDGTest(a,mid+1,end,key);
		}else if(key<a[mid]){
			return searchDGTest(a,start,mid-1,key);
		}

		return -1;
	}

















}

