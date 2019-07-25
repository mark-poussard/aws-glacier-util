package com.kult.aws.glacier.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadResult;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartResult;
import com.amazonaws.util.BinaryUtils;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final int PART_SIZE = 1024 * 1024 * 100;
	private static final String PART_SIZE_STR = String.valueOf(PART_SIZE);
	private static final String VAULT_NAME = "pictures";
	
	private static AmazonGlacier client;
	
    public static void main( String[] args )
    {
    	if(args.length < 1) {
    		System.err.println("Invalid usage, expected at least one file argument.");
    		return ;
    	}
    	client = AmazonGlacierClientBuilder.defaultClient();
    	try {
    		for(String filePath : args) {
	    		System.out.println("Uploading file " + filePath);
    			File file = new File(filePath);
    			if(!file.exists()) {
    	    		System.err.println("Invalid path name " + filePath);
    	    		continue;
    			}
                System.out.println("Uploading an archive.");
                String uploadId = initiateMultipartUpload(file);
                String checksum = uploadParts(file, uploadId);
                String archiveId = CompleteMultiPartUpload(file, uploadId, checksum);
                System.out.println("Completed an archive. ArchiveId: " + archiveId);
                System.out.println("Completed upload of file " + file.getName());
    		}
    	}
    	catch (Exception e){
            System.err.println(e);
    	}
    }
    
    private static String initiateMultipartUpload(File file) {
        // Initiate
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest()
            .withVaultName(VAULT_NAME)
            .withArchiveDescription(file.getName())
            .withPartSize(PART_SIZE_STR);
        
        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
        
        System.out.println("ArchiveID: " + result.getUploadId());
        return result.getUploadId();
    }

    private static String uploadParts(File file, String uploadId) throws AmazonServiceException, NoSuchAlgorithmException, AmazonClientException, IOException {

        int filePosition = 0;
        long currentPosition = 0;
        byte[] buffer = new byte[PART_SIZE];
        List<byte[]> binaryChecksums = new LinkedList<byte[]>();
        
        FileInputStream fileToUpload = new FileInputStream(file);
        String contentRange;
        int read = 0;
        while (currentPosition < file.length())
        {
            read = fileToUpload.read(buffer, filePosition, buffer.length);
            if (read == -1) { break; }
            byte[] bytesRead = Arrays.copyOf(buffer, read);

            contentRange = String.format("bytes %s-%s/*", currentPosition, currentPosition + read - 1);
            String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytesRead));
            byte[] binaryChecksum = BinaryUtils.fromHex(checksum);
            binaryChecksums.add(binaryChecksum);
            System.out.println(contentRange);
                        
            //Upload part.
            UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
            .withVaultName(VAULT_NAME)
            .withBody(new ByteArrayInputStream(bytesRead))
            .withChecksum(checksum)
            .withRange(contentRange)
            .withUploadId(uploadId);               
        
            UploadMultipartPartResult partResult = client.uploadMultipartPart(partRequest);
            System.out.println("Part uploaded, checksum: " + partResult.getChecksum());
            System.out.println("Uploaded " + currentPosition + "/" + file.length() + " of " + file.getName());
            
            currentPosition = currentPosition + read;
        }
        fileToUpload.close();
        String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
        return checksum;
    }

    private static String CompleteMultiPartUpload(File file, String uploadId, String checksum) throws NoSuchAlgorithmException, IOException {
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
            .withVaultName(VAULT_NAME)
            .withUploadId(uploadId)
            .withChecksum(checksum)
            .withArchiveSize(String.valueOf(file.length()));
        
        CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
        return compResult.getLocation();
    }
}
