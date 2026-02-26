<?php

namespace App;

final class Parser
{
    private const BUFFER_SIZE = 32 * 1024 * 1024; // 32 MB
    private const DOMAIN_LEN  = 19;                // strlen('https://stitcher.io')

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Find a clean line boundary near the midpoint
        $mid = $this->findLineStart($inputPath, intdiv($fileSize, 2));

        // Temp file for child's result
        $tmpFile = tempnam(sys_get_temp_dir(), 'parser_');

        $pid = pcntl_fork();

        if ($pid === 0) {
            // Child: second half
            $data = $this->processChunk($inputPath, $mid, $fileSize);
            file_put_contents($tmpFile, serialize($data));
            exit(0);
        }

        // Parent: first half
        $result = $this->processChunk($inputPath, 0, $mid);

        pcntl_waitpid($pid, $status);

        // Merge child (new paths appended in their first-appearance order)
        $childData = unserialize(file_get_contents($tmpFile));
        unlink($tmpFile);

        foreach ($childData as $path => $dates) {
            foreach ($dates as $date => $count) {
                $result[$path][$date] = ($result[$path][$date] ?? 0) + $count;
            }
        }

        // Sort dates ascending within each path
        foreach ($result as &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($result, JSON_PRETTY_PRINT));
    }

    private function findLineStart(string $filePath, int $approxPos): int
    {
        $fh = fopen($filePath, 'rb');
        fseek($fh, $approxPos);
        fgets($fh); // skip partial line → land on next line start
        $pos = ftell($fh);
        fclose($fh);
        return $pos;
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $result   = [];
        $fh       = fopen($filePath, 'rb');
        fseek($fh, $start);

        $remaining = '';
        $filePos   = $start;

        while ($filePos < $end) {
            $toRead = min(self::BUFFER_SIZE, $end - $filePos);
            $buffer = fread($fh, $toRead);
            if ($buffer === false || $buffer === '') break;
            $filePos += strlen($buffer);

            $offset = 0;

            // Handle cross-buffer line: stitch remaining + beginning of new buffer
            if ($remaining !== '') {
                $firstNl = strpos($buffer, "\n");
                if ($firstNl === false) {
                    $remaining .= $buffer;
                    continue;
                }
                $line    = $remaining . substr($buffer, 0, $firstNl);
                $lineLen = strlen($line);
                if ($lineLen >= 46) {
                    $urlPath = substr($line, self::DOMAIN_LEN, $lineLen - 45);
                    $date    = substr($line, $lineLen - 25, 10);
                    $result[$urlPath][$date] = ($result[$urlPath][$date] ?? 0) + 1;
                }
                $remaining = '';
                $offset    = $firstNl + 1;
            }

            $lastNl = -1;

            while (($nlPos = strpos($buffer, "\n", $offset)) !== false) {
                $lineLen = $nlPos - $offset;
                if ($lineLen >= 46) { // domain(19) + path(min 1) + comma(1) + timestamp(25)
                    $urlPath = substr($buffer, $offset + self::DOMAIN_LEN, $lineLen - 45);
                    $date    = substr($buffer, $nlPos - 25, 10);
                    $result[$urlPath][$date] = ($result[$urlPath][$date] ?? 0) + 1;
                }
                $lastNl = $nlPos;
                $offset = $nlPos + 1;
            }

            $remaining = $lastNl >= 0 ? substr($buffer, $lastNl + 1) : $buffer;
        }

        // Handle last line (no trailing newline)
        if ($remaining !== '') {
            $line    = rtrim($remaining, "\r\n");
            $lineLen = strlen($line);
            if ($lineLen >= 46) {
                $urlPath = substr($line, self::DOMAIN_LEN, $lineLen - 45);
                $date    = substr($line, $lineLen - 25, 10);
                $result[$urlPath][$date] = ($result[$urlPath][$date] ?? 0) + 1;
            }
        }

        fclose($fh);
        return $result;
    }
}
